from kafka import KafkaConsumer
import json

# Connect to the Kafka "Conveyor Belt"
consumer = KafkaConsumer(
    'transactions',  # <--- Matches your Producer's topic name
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("👮 Fraud Detector Started... Waiting for transactions...")

for message in consumer:
    transaction = message.value
    amount = transaction['amount']
    city = transaction['city']
    
    # RULE: If amount is > 40,000, flag it!
    if amount > 40000:
        print(f"🚨 FRAUD ALERT! High Value: ₹{amount} in {city}")
    else:
        print(f"✅ Legit: ₹{amount}")