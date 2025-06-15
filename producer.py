from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    data = {
        "id": random.choice(["room1", "room2"]),
        "temperature": round(random.uniform(50, 65), 2),
        "humidity": round(random.uniform(40, 70), 2),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send('building_sensors', value=data)
    print(f"Sent: {data}")  # <-- Цей рядок додай
    time.sleep(1)
