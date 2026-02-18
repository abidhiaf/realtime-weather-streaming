import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

API_KEY = "C09a0cbd81dbeb39eb6c1e1e138e5ae6"

CITIES = [
    "Paris",
    "Lyon",
    "Marseille",
    "Toulouse",
    "Bordeaux",
    "Lille",
    "Nice",
    "Nantes",
    "Strasbourg",
    "Montpellier"
]

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_weather(city):
    try:
        params = {
            "q": city,
            "appid": API_KEY,
            "units": "metric"
        }

        response = requests.get(BASE_URL, params=params, timeout=5)

        if response.status_code == 200:
            data = response.json()

            event = {
                "city": city,
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "event_time": datetime.utcnow().isoformat()
            }

            producer.send("raw_api_events", value=event)
            print("Message envoyé :", event)

        else:
            print(f"Erreur API pour {city} :", response.status_code)

    except Exception as e:
        print(f"Erreur pour {city} :", e)

if __name__ == "__main__":
    while True:
        for city in CITIES:
            fetch_weather(city)
        time.sleep(10)
