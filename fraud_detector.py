from kafka import KafkaConsumer
import json
import joblib
import numpy as np
import os

print(" Loading Fraud Detection Model...")
try:
    model = joblib.load('fraud_model.pkl')
    print(" Model Loaded Successfully!")
except:
    print(" Error: You forgot to run 'python train_model.py' first!")
    exit()

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(" Smart Fraud Detector Started... Watching for trancastions")

for message in consumer:
    transaction = message.value
    
    features = np.array([[transaction['amount']]])
    
    prediction = model.predict(features)

    if prediction[0] == 1:
        alert_msg = f" FRAUD DETECTED: ₹{transaction['amount']} in {transaction['city']}"
        print(alert_msg)
        
        with open("fraud_logs.txt", "a") as log_file:
            log_file.write(f"{alert_msg}\n")
            
    else:
        print(f" Legit: ₹{transaction['amount']}")