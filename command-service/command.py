import json
import os
import asyncio
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
import logging
import socket

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _get_env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _as_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


APP_NAME = "Connected Car Ingest API"
KAFKA_BROKERS = _get_env("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = _get_env("KAFKA_TOPIC", "car.telemetry.events")
KAFKA_CLIENT_ID = _get_env("KAFKA_CLIENT_ID", "command-api-producer")
KAFKA_ENABLED = _as_bool("KAFKA_ENABLED", False) # 로컬 테스트용으로 False
ENABLE_SCHEMA_LOG = _as_bool("ENABLE_SCHEMA_LOG", False)

# 전역 비동기 카프카 프로듀서
producer: Optional[AIOKafkaProducer] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 켜질 때 
    global producer
    if KAFKA_ENABLED:
        try:
            unique_client_id = f"{KAFKA_CLIENT_ID}-{socket.gethostname()}"

            producer = AIOKafkaProducer(
                bootstrap_servers = [s.strip() for s in KAFKA_BROKERS.split(",") if s.strip()],
                value_serializer = lambda v : json.dumps(v).encode("utf-8"),
                key_serializer = lambda v : v.encode("utf-8") if isinstance(v, str) else None,
                acks = "all",
                client_id = unique_client_id,
                compression_type = "gzip"
            )
            await producer.start()
            logger.info(f"[AIOKafkaProducer] started: brokers = {KAFKA_BROKERS}, client_id = {unique_client_id}")
        
        except Exception as exc:
            logger.error(f"[AIOKafkaProducer] init failed : {exc}")
            producer = None
    else:
        logger.info(" Kafka disabled - running in local test mode")

    yield

    # 서버 꺼질 때
    if producer:
        await producer.stop()
        logger.info("AIOKafkaProducer stopped")

app = FastAPI(title = APP_NAME, lifespan = lifespan)

class VehicleInfo(BaseModel):
    vehicle_id: str
    vin: str
    model: str
    driver: str
    timestamp: str

class DynamicsInfo(BaseModel):
    traction: str
    acceleration_mps2: float
    brake_pct: float
    throttle_pct: float
    steering_deg: float
    gear: str
    driving_mode: str

class StatusInfo(BaseModel):
    is_locked: bool
    ignition_on: bool
    park_brake: bool
    door_locked: Dict[str, bool]
    window_position: Dict[str, str]

class DiagnosticsInfo(BaseModel):
    warnings: Dict[str, bool]
    tire_pressure_psi: Dict[str, float]
    firmware_ver: str
    sensor_health: str

class Coordinates(BaseModel):
    latitude: float
    longitude: float


class LocationInfo(BaseModel):
    city: str
    coordinates: Coordinates
    heading_deg: float
    altitude_m: float
    gps_accuracy_m: float


class TripInfo(BaseModel):
    state: str
    duration_s: int
    duration_hms: str
    speed_kmh: float
    odometer_km: float
    odometer_delta_km: float


class BatteryInfo(BaseModel):
    soc_pct: float
    health_pct: float
    pack_voltage_v: float
    pack_current_a: float
    aux_12v_battery_v: float
    is_charging: bool


class ConnectedCarData(BaseModel):
    vehicle: VehicleInfo
    location: LocationInfo
    trip: TripInfo
    battery: BatteryInfo
    temperatures_c: Dict[str, float]
    dynamics: DynamicsInfo
    status: StatusInfo
    diagnostics: DiagnosticsInfo
    events: List[str]


def _build_kafka_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    # 원본 데이터를 Kafka 메시지 포맷으로 변환
    vehicle = payload["vehicle"]
    location = payload["location"]["coordinates"]
    trip = payload["trip"]
    battery = payload["battery"]
    events = payload.get("events", [])

    return {
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": vehicle["timestamp"],
        "received_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "state": trip.get("state"),
        "speed_kmh": trip.get("speed_kmh"),
        "soc_pct": battery.get("soc_pct"),
        "location": {
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
        },
        "recent_event": events[-1] if events else None,
        "raw": payload,
    }

async def _send_to_kafka(data_list: List[Dict[str, Any]]) -> int:
    # Kafka로 데이터 전송
    if not KAFKA_ENABLED or producer is None: # 카프카가 죽거나 producer가 없을 때
        # 로컬 모드: 로그만 출력하고 성공 처리
        logger.info(f"[LOCAL MODE] Would send {len(data_list)} messages to Kafka")

        if ENABLE_SCHEMA_LOG and data_list:
            logger.debug(f"Sample payload: {json.dumps(data_list[0], ensure_ascii=False)[:300]}")
        return len(data_list)

    # Kafka 전송
    tasks = []
    for data in data_list:
        kafka_payload = _build_kafka_message(data)

        if ENABLE_SCHEMA_LOG:
            logger.debug(f"Sending to Kafka: {kafka_payload['vehicle_id']} @ {kafka_payload['timestamp']}")

        tasks.append(
            producer.send_and_wait(
                KAFKA_TOPIC,
                key = kafka_payload["vehicle_id"],
                value = kafka_payload
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions = True)

    failed = [(data_list[idx], error) for idx, error in enumerate(results) if isinstance(error, Exception)]
    success_count = len(data_list) - len(failed)

    if failed:
        for data, exc in failed:
            vehicle_id = data.get("vehicle", {}).get("vehicle_id", "unknown")
            logger.error(f"Kafka 전송 실패 vehicle_id = {vehicle_id}: {exc}")

    if success_count == 0:
        raise HTTPException(status_code = 500, detail = "모든 메시지 Kafka 전송 실패")

    logger.info(f"Kafka 전송 완료: 성공 {success_count}건 / 실패 {len(failed)}건") 
    return success_count

@app.post("/api/telemetry/batch")
async def ingest_telemetry_batch(data_list: List[ConnectedCarData]):
    # 배치 텔레메트리 수신 (단일 데이터도 리스트로 받음)

    if not data_list:
        raise HTTPException(status_code = 400, detail = "Empty data list")
    
    # Pydantic 모델을 딕셔너리로 변환
    payloads = [data.model_dump() for data in data_list]

    # Kafka로 전송
    processed_count = await _send_to_kafka(payloads)

    # 응답
    vehicle_ids = [p["vehicle"]["vehicle_id"] for p in payloads]

    return {
        "status": "success" if processed_count == len(data_list) else "partial",
        "processed_count": processed_count,
        "failed_count" : len(data_list) - processed_count,
        "vehicle_ids": vehicle_ids[:10] if len(vehicle_ids) > 10 else vehicle_ids,
        "kafka_enabled": KAFKA_ENABLED,
    }

@app.post("/api/telemetry")
async def ingest_telemetry_single(data: ConnectedCarData):
    # 단일 텔레메트리 수신 (하위 호환성용)
    # 내부적으론 배치 엔드포인트 호출

    result = await ingest_telemetry_batch([data])

    return {
        "status" : "success",
        "processed_vehicle" : data.vehicle.vehicle_id,
        "kafka_enabled": KAFKA_ENABLED,
    }


@app.get("/health")
async def health_check():
    kafka_status = "connected" if (KAFKA_ENABLED and producer) else "disabled"

    return {
        "status": "ok",
        "service": APP_NAME,
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_status" : kafka_status,
        "kafka_topic": KAFKA_TOPIC if KAFKA_ENABLED else "N/A",
        "kafka_brokers": KAFKA_BROKERS if KAFKA_ENABLED else "N/A",
    }

@app.get("/")
async def root():
    # API 정보
    return {
        "service": APP_NAME,
        "version": "1.0.0",
        "endpoints": {
            "batch": "/api/telemetry/batch",
            "single": "/api/telemetry",
            "health": "/health"
        },
        "kafka_enabled": KAFKA_ENABLED,
    }