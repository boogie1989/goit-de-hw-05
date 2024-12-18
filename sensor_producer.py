import json
import uuid
import time
import random
from confluent_kafka import Producer
from kafka_config import KAFKA_CONFIG
from colorama import Fore, Style, init

init(autoreset=True)

def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
        "security.protocol": KAFKA_CONFIG["security.protocol"],
        "sasl.mechanism": KAFKA_CONFIG["sasl.mechanism"],
        "sasl.username": KAFKA_CONFIG["username"],
        "sasl.password": KAFKA_CONFIG["password"],
    })

def generate_sensor_data():
    sensor_id = str(uuid.uuid4())
    temperature = random.randint(25, 45)
    humidity = random.randint(15, 85)
    timestamp = int(time.time())

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
    }

def send_data(producer, topic, data):
    producer.produce(
        topic,
        key=data["sensor_id"],
        value=json.dumps(data),
        callback=lambda err, msg: print(
            f"{Fore.GREEN}Повідомлення надіслано: {msg.value().decode('utf-8')} до {msg.topic()}{Style.RESET_ALL}"
            if err is None
            else f"{Fore.RED}Помилка: {err}{Style.RESET_ALL}"
        ),
    )
    producer.flush()

def main():
    producer = create_producer()
    data = generate_sensor_data()

    print(f"{Fore.YELLOW}Генерація даних для датчика {Fore.CYAN}{data['sensor_id']}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Температура: {data['temperature']}°C, Вологість: {data['humidity']}%{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Час: {data['timestamp']}{Style.RESET_ALL}")

    topic_name = "building_sensors"
    send_data(producer, topic_name, data)

    print(f"{Fore.BLUE}Дані надіслано до топіку {topic_name}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
