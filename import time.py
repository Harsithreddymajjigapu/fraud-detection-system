import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("💳 Credit Card Terminal Started... (Press Ctrl+C to stop)")

while True:
    transaction = {
        "id": fake.uuid4(),
        "name": fake.name(),
        "amount": round(random.uniform(10, 50000), 2),
        "city": fake.city(),
        "timestamp": time.time()
    }

    producer.send('transactions', value=transaction)
    print(f"Sent: ₹{transaction['amount']} from {transaction['city']}")
    time.sleep(1)