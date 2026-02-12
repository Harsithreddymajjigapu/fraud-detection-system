import pymongo
from datetime import datetime
import random

# 1. Connect to MongoDB (Localhost or Atlas Cloud)
# If using a local installed MongoDB:
client = pymongo.MongoClient("mongodb://localhost:27017/")

# 2. Create the Database
# MongoDB creates the DB automatically when you first store data in it.
db = client["fraud_detection_system"]

# 3. Create the Collection (Table)
transactions_col = db["live_transactions"]

# 4. OPTIONAL: Create Indexes for Speed (Crucial for Real-Time systems)
# We index 'user_id' and 'timestamp' so we can quickly check velocity 
# (e.g., "How many transactions did User X do in the last 10 mins?")
transactions_col.create_index([("user_id", pymongo.ASCENDING)])
transactions_col.create_index([("timestamp", pymongo.DESCENDING)])
print("Database and Indexes configured successfully.")

# ==========================================
# 5. The Schema: Inserting a "Real-Life" Record
# ==========================================

def log_transaction(user_id, amount, merchant, device_info, location):
    """
    Inserts a transaction with rich metadata for fraud analysis.
    """
    transaction_document = {
        "transaction_id": f"TXN_{random.randint(100000, 999999)}",
        "user_id": user_id,
        "amount": amount,
        "currency": "INR",
        "timestamp": datetime.now(), # Crucial for velocity checks
        
        # MERCHANT INFO (Who are they paying?)
        "merchant": {
            "name": merchant,
            "category": "Electronics", # e.g., High risk category
            "merchant_id": "M_5521"
        },
        
        # DEVICE FINGERPRINT (The "Invisible" Layer)
        # This identifies IF the user is on their usual phone or a strange laptop
        "device_fingerprint": {
            "device_id": device_info.get("device_id"), # Unique Hash of hardware
            "ip_address": device_info.get("ip"),
            "os": device_info.get("os"), # Android 14, Windows 11
            "is_vpn": device_info.get("is_vpn", False), # Flag for VPN users
            "battery_level": device_info.get("battery", 100) # Bots often show 100% constant
        },

        # LOCATION (The "Impossible Travel" Check)
        "geo_location": {
            "latitude": location.get("lat"),
            "longitude": location.get("long"),
            "city": location.get("city"),
            "country": "India"
        },

        # FRAUD LABELS (For Training Later)
        "is_fraud": False,  # Default to False
        "risk_score": 0.0   
    }

    result = transactions_col.insert_one(transaction_document)
    print(f"Transaction Logged! ID: {result.inserted_id}")

device_data = {
    "device_id": "8329-adj-219a",
    "ip": "157.32.11.0",
    "os": "Android 14",
    "is_vpn": False,
    "battery": 78
}

loc_data = {
    "lat": 17.3850,
    "long": 78.4867,
    "city": "Hyderabad"
}

log_transaction(
    user_id="USER_007", 
    amount=25000.00, 
    merchant="MG Motors Accessories", 
    device_info=device_data, 
    location=loc_data
)