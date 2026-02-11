from kafka import KafkaConsumer
import json
import numpy as np
import joblib
import os
from tensorflow.keras.models import load_model

print("🧠 Loading Deep Learning Model & Scaler...")
try:
    model = load_model('fraud_dl_model.keras')
    scaler = joblib.load('scaler.pkl')
    print("✅ Neural Network Loaded Successfully!")
except:
    print("❌ Error: Files missing! Did you run 'python train_deep_model.py'?")
    exit()

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("👮 Deep Learning Detector Started... Analyzing Risk Probabilities...")

for message in consumer:
    transaction = message.value
    
    features = np.array([[transaction['amount']]])
    features_scaled = scaler.transform(features)
    
    risk_score = model.predict(features_scaled, verbose=0)[0][0]
    
    if risk_score > 0.50:
        confidence_percent = risk_score * 100
        alert_msg = f"🚨 FRAUD DETECTED: ₹{transaction['amount']} (Risk: {confidence_percent:.2f}%)"
        print(alert_msg)
        
        with open("fraud_logs.txt", "a", encoding='utf-8') as f:
            f.write(f"{alert_msg} in {transaction['city']}\n")
            
    else:
        safe_percent = (1 - risk_score) * 100
        print(f"✅ Legit: ₹{transaction['amount']} (Safe: {safe_percent:.2f}%)")