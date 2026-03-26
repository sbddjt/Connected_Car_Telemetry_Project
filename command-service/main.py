import json
import os
import asyncio
from datetime import datetime, timezone, timedelta
from aiokafka import AIOKafkaProducer
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

KST = timezone(timedelta(hours=9))


def _get_env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _as_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


APP_NAME = "Connected Car Ingest API"
KAFKA_BROKERS = _get_env("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = _get_env("KAFKA_TOPIC", "car.telemetry.events")
KAFKA_CLIENT_ID = _get_env("KAFKA_CLIENT_ID", "command-api-producer")
KAFKA_ENABLED = _as_bool("KAFKA_ENABLED", True)
ENABLE_SCHEMA_LOG = _as_bool("ENABLE_SCHEMA_LOG", False)

app = FastAPI(title=APP_NAME)

# 전역 비동기 카프카 프로듀서
producer: Optional[AIOKafkaProducer] = None

@app.on_event("startup")
async def startup_event():
    global producer
    if KAFKA_ENABLED:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers = [s.strip() for s in KAFKA_BROKERS.split(",") if s.strip()],
                value_serializer = lambda v : json.dumps(v).encode("utf-8"),
                key_serializer = lambda v : v.encode("utf-8") if isinstance(v, str) else None,
                acks = "all",
                client_id = KAFKA_CLIENT_ID,
            )

            await producer.start() # 비동기로 연결 시작
            print(f"[AIOKafkaProducer] started: brokers = {KAFKA_BROKERS}")
        
        except Exception as exc:
            print(f"[AIOKafkaProducer] init failed : {exc}")
            producer = None

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()
        print("[AIOKafkaProducer] stopped")


class VehicleInfo(BaseModel):
    vehicle_id: str
    vin: str
    model: str
    driver: str
    timestamp: str


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
    dynamics: Dict[str, Any]
    status: Dict[str, Any]
    diagnostics: Dict[str, Any]
    events: List[str]


def _build_kafka_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    vehicle = payload["vehicle"]
    location = payload["location"]["coordinates"]
    trip = payload["trip"]
    battery = payload["battery"]
    events = payload.get("events", [])

    return {
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": vehicle["timestamp"],
        "received_at": datetime.now(KST).isoformat(timespec="seconds"),
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

@app.post("/api/telemetry/batch")
async def ingest_telemetry_batch(data_list: List[ConnectedCarData]):
    global producer

    # 로컬 연습용 코드
    return {"status": "success", "processed_count": len(data_list)}

    if not KAFKA_ENABLED or producer is None:
        raise HTTPException(
            status_code = 503,
            detail = "Kafka producer is not available. Check configuration."
        )

    tasks = []

    # 데이터를 루프 돌며 전송 예약을 걸어둠.
    for data in data_list:
        payload = data.model_dump()
        kafka_payload = _build_kafka_message(payload)

        # 비동기 send_and_wait() 방식 사용
        tasks.append(
            producer.send_and_wait(
                KAFKA_TOPIC,
                key = payload["vehicle"]["vehicle_id"],
                value = kafka_payload
            )
        )

    try:
        # 예약된 전송 작업을 병렬로 쏴버림
        await asyncio.gather(*tasks)
        return {"status": "success", "processed_count": len(data_list)}
    
    except Exception as exc:
        print(f"[ingest/batch] send failed: {exc}")
        raise HTTPException(status_code = 500, detail = "Failed to publish telemetry event batch.")

@app.post("/api/telemetry")
async def ingest_telemetry_single(data: ConnectedCarData):
    global producer

    # 로컬 연습용 코드
    return {"status": "success", "processed_vehicle": data.vehicle.vehicle_id}

    if not KAFKA_ENABLED or producer is None:
        raise HTTPException(
            status_code = 503,
            detail = "Kafka producer is not available. Check configuration."
        )

    payload = data.model_dump()
    if ENABLE_SCHEMA_LOG:
        print("[ingest] payload:", json.dumps(payload, ensure_ascii=False)[:500])

    kafka_payload = _build_kafka_message(payload)

    try:
        await producer.send_and_wait(
            KAFKA_TOPIC,
            key = payload["vehicle"]["vehicle_id"],
            value = kafka_payload,
        )

        return {"status": "success", "processed_vehicle": payload["vehicle"]["vehicle_id"]}
    
    except Exception as exc:
        print(f"[ingest/single] send failed: {exc}")
        raise HTTPException(status_code = 500, detail = "Failed to publish telemetry event.")

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": APP_NAME,
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_brokers": KAFKA_BROKERS,
    }
