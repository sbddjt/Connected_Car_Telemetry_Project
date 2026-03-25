import asyncio
import httpx
import random
from collections import deque
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
COMMAND_SERVICE_URL = "http://localhost:8000/telemetry"
CAR_COUNT = 10 # 테스트용 차량 대수
KST = timezone(timedelta(hours=9)) # 서울 시간 설정

class DummyCar:
    # __init__: 객체 생성 시 초기화 (생성자)
    # 차량마다 고유한 ID와 시작 위치, 초기 물리량을 설정합니다.

    def __init__(self, car_id):
        self.car_id = car_id
        self.lat = 37.5665  # 서울시청 위도
        self.lng = 126.9780 # 서울시청 경도
        self.speed = 40.0    # 초기 속도 (km/h)
        self.heading = random.uniform(0, 360) # 초기 방향
        self.accel = 0.0     # 초기 가속도
        self.seq = 0         # 메시지 순번 (유실 확인용)

        self.buffer = deque(maxlen=1000) # 데이터를 잠시 보관할 로컬 버퍼 (최대 1000개 유지)

    def move(self):
        # 차량의 물리적 움직임을 시뮬레이션합니다.
        # random.uniform을 사용하여 소수점 단위의 부드러운 이동을 구현합니다.
        self.seq += 1

        # 가속도 및 속도 변화 (경로 예측을 위한 데이터)
        self.accel = round(random.uniform(-1.5, 1.5), 2)
        self.speed = max(0, min(110, self.speed + self.accel))

        # 방향 변화 (커브길 주행 시뮬레이션)
        self.heading = (self.heading + random.uniform(-10, 10)) % 360

        # 좌표 이동 계산 (매우 단순화된 물리 모델)
        move_factor = self.speed * 0.000005 # 이동 계수
        self.lat += move_factor * random.uniform(-0.1, 0.1)
        self.lng += move_factor * random.uniform(-0.1, 0.1)

        payload = {
            "car_id": self.car_id,
            "seq": self.seq,
            "latitude": round(self.lat, 6),
            "longitude": round(self.lng, 6),
            "speed": round(self.speed, 2),
            "accel": self.accel,
            "heading": round(self.heading, 1),
            "timestamp": datetime.now(KST).isoformat()
        } 

        # 데이터가 생성되면 무조건 일단 버퍼에 집어넣음 (서버 다운 시 데이터 유실 방지)
        self.buffer.append(payload)

async def send_telemetry(client, car):
    if not car.buffer:
        return # 버퍼가 비어있으면 아무것도 안하면 됨
    
    # 버퍼에 쌓인 데이터를 리스트에 복사함.
    # 평소라면 1개씩, 쌓여있었다면 여러 개가 있을 것
    bulk_data = list(car.buffer)
    batch_size = len(bulk_data)

    try:
        # '/telemetry/batch' 주소로 리스트 전체를 한 번에 쏨
        # 데이터가 많을 수 있으니 timeout을 넉넉하게 5초를 줌   
        response = await client.post(
            COMMAND_SERVICE_URL + "/batch",
            json = bulk_data,
            timout = 5.0
        )

        if response.status_code == 200:
            print(f"✅ [{car.car_id}] {batch_size}건 한 방에 전송 완료!")

            # 전송에 성공한 개수만큼만 버퍼에서 빼냄
            for _ in range(batch_size):
                car.buffer.popleft()
        
        else:
            print(f"⚠️ [{car.car_id}] 서버 응답 에러 ({response.status_code})")

    except Exception as e:
        print(f"❌ [{car.car_id}] 연결 실패 (현재 대기열: {len(car.buffer)}개) - 에러: {e}")
                    
async def main():
    # 여러 대의 차량 객체 생성
    cars = [DummyCar(f"CAR-{i:03d}") for i in range(1, CAR_COUNT + 1)]

    print(f"🚀 {CAR_COUNT}대의 차량 시뮬레이션을 시작합니다. (대상: {COMMAND_SERVICE_URL})")

    # AsyncClient를 사용하여 효율적인 비동기 통신
    async with httpx.AsyncClient() as client:
        while True:
            tasks = []
            for car in cars:
                car.move() # 차를 움직여서 데이터를 버퍼에 쌓음
                tasks.append(send_telemetry(client, car)) # 버퍼 비우기 시도

            # 모든 차량의 전송 작업을 동시에 실행 (await)
            await asyncio.gather(*tasks)

            # 1초 주기로 전송
            await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 시뮬레이션을 종료합니다.")
