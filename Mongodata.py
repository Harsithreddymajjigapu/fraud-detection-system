import pymongo
from datetime import datetime
import random

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["fraud_detection_system"]

transactions_col = db["live_transactions"]

transactions_col.create_index([("user_id", pymongo.ASCENDING)])
transactions_col.create_index([("timestamp", pymongo.DESCENDING)])
print("Database and Indexes configured successfully.")

def log_transaction(user_id, amount, merchant, device_info, location):
    """
    Inserts a transaction with rich metadata for fraud analysis.
    """
    transaction_document = {
        "transaction_id": f"TXN_{random.randint(100000, 999999)}",
        "user_id": user_id,
        "amount": amount,
        "currency": "INR",
        "timestamp": datetime.now(),
        "merchant": {
            "name": merchant,
            "category": "Electronics", 
            "merchant_id": "M_5521"
        },
        
        "device_fingerprint": {
            "device_id": device_info.get("device_id"), 
            "ip_address": device_info.get("ip"),
            "os": device_info.get("os"), 
            "is_vpn": device_info.get("is_vpn", False),
            "battery_level": device_info.get("battery", 100)
        },


        "geo_location": {
            "latitude": location.get("lat"),
            "longitude": location.get("long"),
            "city": location.get("city"),
            "country": "India"
        },

        "is_fraud": False,  
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