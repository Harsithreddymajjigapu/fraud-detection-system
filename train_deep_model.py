import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from sklearn.preprocessing import StandardScaler
import joblib

print("🧠 Initializing Deep Learning Training...")

# --- 1. GENERATE DATA (The Textbooks) ---
np.random.seed(42)

# Legit: 800 transactions between ₹10 and ₹10,000
legit_amounts = np.random.randint(10, 10000, 800)
legit_labels = np.zeros(800) # 0 = Legit

# Fraud: 200 transactions between ₹30,000 and ₹1,00,000
fraud_amounts = np.random.randint(30000, 100000, 200)
fraud_labels = np.ones(200) # 1 = Fraud

# Combine & Reshape
X = np.concatenate([legit_amounts, fraud_amounts]).reshape(-1, 1)
y = np.concatenate([legit_labels, fraud_labels])

# --- 2. PREPROCESSING (Scaling) ---
# Neural Networks fail with big numbers. We must scale them to be near -1 to 1.
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# --- 3. BUILD THE MODEL (The Brain Structure) ---
model = Sequential([
    # Input Layer: Accepts 1 feature (Amount)
    Dense(16, activation='relu', input_shape=(1,)), 
    # Hidden Layer: The "Thinking" layer
    Dense(8, activation='relu'),
    # Output Layer: Returns probability (0.0 to 1.0)
    Dense(1, activation='sigmoid')
])

# --- 4. COMPILE & TRAIN ---
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

print("🏋️ Training starting (20 Epochs)...")
model.fit(X_scaled, y, epochs=20, batch_size=32, verbose=1)

# --- 5. SAVE THE ARTIFACTS ---
model.save('fraud_dl_model.keras')
joblib.dump(scaler, 'scaler.pkl')

print("\n✅ SUCCESS! Deep Learning Model saved as 'fraud_dl_model.keras'")
print("✅ Scaler saved as 'scaler.pkl'")