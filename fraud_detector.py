from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions', 
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("👮 Fraud Detector Started... Waiting for transactions...")

for message in consumer:
    transaction = message.value
    amount = transaction['amount']
    city = transaction['city']

    if amount > 40000:
        sms.send(phone_number, "Did you just spend ₹40,000? Reply YES or NO.")
    else:
        print(f"✅ Legit: ₹{amount}")