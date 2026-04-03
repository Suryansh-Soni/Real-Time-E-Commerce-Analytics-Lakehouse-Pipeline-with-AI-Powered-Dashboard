from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ["Shoes", "Laptop", "Phone", "Watch", "Headphones"]

while True:
    data = {
        "order_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "price": random.randint(500, 50000),
        "quantity": random.randint(1, 3),
        "timestamp": str(datetime.now())
    }

    producer.send("ecommerce_orders", data)
    print("Sent:", data)

    time.sleep(.2)