from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
from typing import List
import asyncio
import json
import os

app = FastAPI()

# 데이터 규격 정의 (Pydantic)
class TelemetryData(BaseModel):
    car_id : str
    seq: int
    latitude : float
    longitude: float
    speed: float
    accel: float
    heading: float
    timestamp: str

# Kafka 설정 (환경변수나 기본값 사용)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
TOPIC_NAME = "car.telemetry.v1"

# 글로벌 Producer 변수
producer = None

@app.on_event("startup")
async def startup_event():
    # 앱 시작 시 Kafka Producer를 초기화합니다.
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,

        # 유실 방지를 위한 핵심 설정
        acks = 'all',
        enable_idempotence=True, # 중복 전송 방지
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    await producer.start()
    print(f"🚀 Kafka Producer 시작됨 (Servers: {KAFKA_BOOTSTRAP_SERVERS})")

@app.on_event("shutdown")
async def shutdown_event():
    # 앱 종료 시 Producer를 안전하게 닫습니다.
    await producer.stop()

# 1건씩 처리
@app.post("/telemetry")
async def receive_telemetry(data: TelemetryData):
    # 더미 데이터로부터 차량 정보를 받아 Kafka로 전송합니다.
    try:
        # Kafka로 전송
        # car_id를 key로 지정하면 특정 차량 데이터는 항상 같은 파티션에 쌓여 순서가 보장됨.
        await producer.send_and_wait(
            TOPIC_NAME,
            value = data.dict(),
            key = data.car_id.encode('utf-8')
        )

        return {"status": "success", "car_id": data.car_id, "seq": data.seq}
    
    except Exception as e:
        print(f"❌ Kafka 전송 에러: {e}")
        raise HTTPException(status_code = 500, detail = "Internal Server Error")

# 여러 건 한 방에 처리 (batch) 
@app.post("/telemetry/batch")
async def receive_telemetry_batch(data_list: List[TelemetryData]):
    try:
        tasks = []
        # 받은 데이터 리스트를 루프 돌며 카프카 전송 예약
        for data in data_list:
            tasks.append(
                producer.send_and_wait(
                    TOPIC_NAME,
                    value = data.dict()
                    key = data.car_id.encode('utf-8')
                )
            )

        # 예약된 카프카 전송을 동시에 병렬로
        await asyncio.gather(*tasks)

        return {"status": "success", "inserted": len(data_list)}
    
    except Exception as e:
        print(f"❌ Kafka Batch 전송 에러: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
