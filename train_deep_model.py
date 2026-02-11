import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from sklearn.preprocessing import StandardScaler
import joblib

print(" Initializing Deep Learning Training...")

np.random.seed(42)

legit_amounts = np.random.randint(10, 10000, 800)
legit_labels = np.zeros(800)

fraud_amounts = np.random.randint(30000, 100000, 200)
fraud_labels = np.ones(200) 
X = np.concatenate([legit_amounts, fraud_amounts]).reshape(-1, 1)
y = np.concatenate([legit_labels, fraud_labels])

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

model = Sequential([
    Dense(16, activation='relu', input_shape=(1,)), 
    Dense(8, activation='relu'),
    Dense(1, activation='sigmoid')
])

model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

print("🏋️ Training starting (20 Epochs)...")
model.fit(X_scaled, y, epochs=20, batch_size=32, verbose=1)

model.save('fraud_dl_model.keras')
joblib.dump(scaler, 'scaler.pkl')

print("\nSUCCESS! Deep Learning Model saved as 'fraud_dl_model.keras'")
print(" Scaler saved as 'scaler.pkl'")