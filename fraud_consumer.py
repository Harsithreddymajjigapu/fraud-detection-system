from kafka import KafkaConsumer
import json
import numpy as np
import joblib
import pymongo
from datetime import datetime
from tensorflow.keras.models import load_model

# ==========================================
# 1. DATABASE CONNECTION (From your Mongodata.py)
# ==========================================
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fraud_detection_system"]
    collection = db["live_transactions"]
    
    # Ensure indexes exist (Good practice to run this every startup)
    collection.create_index([("user_id", pymongo.ASCENDING)])
    collection.create_index([("timestamp", pymongo.DESCENDING)])
    print("✅ Connected to MongoDB & Indexes Verified!")
except Exception as e:
    print(f"❌ Database Error: {e}")
    exit()

# ==========================================
# 2. LOAD AI MODEL (From your training script)
# ==========================================
print("🧠 Loading Deep Learning Model...")
try:
    model = load_model('fraud_dl_model.keras')
    scaler = joblib.load('scaler.pkl')
    print("✅ Neural Network Loaded Successfully!")
except:
    print("❌ Error: Model files missing! Run 'python train_deep_model.py' first.")
    exit()

# ==========================================
# 3. KAFKA CONSUMER SETUP
# ==========================================
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("👮 Fraud Detection System Active. Waiting for live stream...")

# ==========================================
# 4. REAL-TIME PROCESSING LOOP
# ==========================================
for message in consumer:
    txn = message.value
    
    # --- A. Feature Engineering (Prepare data for AI) ---
    # We only trained on 'amount', so we extract just that.
    features = np.array([[txn['amount']]])
    features_scaled = scaler.transform(features)
    
    # --- B. AI Prediction ---
    # The model gives a probability between 0 and 1
    risk_score = float(model.predict(features_scaled, verbose=0)[0][0])
    is_fraud = risk_score > 0.50  # Threshold
    
    # --- C. Create the Document for MongoDB ---
    # We combine the raw transaction + the AI's opinion
    db_record = {
        "transaction_id": txn['id'],
        "user_name": txn['name'],
        "amount": txn['amount'],
        "currency": "INR",
        "city": txn['city'],
        "timestamp": datetime.fromtimestamp(txn['timestamp']), # Convert unix time to real date
        
        # We add the AI analysis here
        "ai_analysis": {
            "risk_score": risk_score,
            "is_blocked": is_fraud,
            "model_version": "v1.0"
        },
        
        # Simulating device data since the Producer doesn't send it yet
        "device_fingerprint": {
            "ip": "192.168.1.10", 
            "os": "Android",
            "is_vpn": False
        }
    }

    # --- D. Save to Database ---
    try:
        collection.insert_one(db_record)
        
        # --- E. Console Output ---
        if is_fraud:
            print(f"🚨 BLOCKED: ₹{txn['amount']} in {txn['city']} (Risk: {risk_score:.2f}) -> 💾 Saved to DB")
        else:
            print(f"✅ APPROVED: ₹{txn['amount']} in {txn['city']} (Risk: {risk_score:.2f}) -> 💾 Saved to DB")
            
    except Exception as e:
        print(f"❌ Error saving to DB: {e}")