import json
import time
from confluent_kafka import Consumer, Producer
from kafka_config import KAFKA_CONFIG
from colorama import Fore, Style, init

init(autoreset=True)

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
        "security.protocol": KAFKA_CONFIG["security.protocol"],
        "sasl.mechanism": KAFKA_CONFIG["sasl.mechanism"],
        "sasl.username": KAFKA_CONFIG["username"],
        "sasl.password": KAFKA_CONFIG["password"],
        "group.id": "sensor_group",
        "auto.offset.reset": "earliest",
    })

def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
        "security.protocol": KAFKA_CONFIG["security.protocol"],
        "sasl.mechanism": KAFKA_CONFIG["sasl.mechanism"],
        "sasl.username": KAFKA_CONFIG["username"],
        "sasl.password": KAFKA_CONFIG["password"],
    })

def process_message(message, producer):
    data = json.loads(message.value().decode("utf-8"))
    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]
    timestamp = data["timestamp"]

    print(Fore.GREEN + f"Отримані дані від датчика {sensor_id}:")
    print(Fore.YELLOW + f"Температура: {temperature}°C, Вологість: {humidity}%")
    print("-" * 50)

    if temperature > 40:
        alert = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": timestamp,
            "message": "Температура перевищує допустимий рівень!",
        }
        print(Fore.RED + "АЛЕРТ: Температура перевищує допустимий рівень! Надсилаємо сповіщення...")
        send_alert(producer, "temperature_alerts", alert)

    if humidity > 80 or humidity < 20:
        alert = {
            "sensor_id": sensor_id,
            "humidity": humidity,
            "timestamp": timestamp,
            "message": "Вологість виходить за допустимі межі!",
        }
        print(Fore.CYAN + "АЛЕРТ: Вологість виходить за допустимі межі! Надсилаємо сповіщення...")
        send_alert(producer, "humidity_alerts", alert)

def send_alert(producer, topic, alert):
    producer.produce(
        topic,
        key=alert["sensor_id"],
        value=json.dumps(alert),
        callback=lambda err, msg: print(
            f"{Fore.GREEN}Сповіщення надіслано до {msg.topic()}{Style.RESET_ALL}"
            if err is None
            else f"{Fore.RED}Помилка при відправці сповіщення: {err}{Style.RESET_ALL}"
        ),
    )
    producer.flush()
    print(Fore.WHITE + "Відфільтровані дані успішно надіслані до відповідного топіку.")
    print("-" * 50)

def main():
    consumer = create_consumer()
    producer = create_producer()

    consumer.subscribe(["building_sensors"])

    try:
        timeout = 60  # Таймаут в секундах
        start_time = time.time()

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                print(Fore.RED + f"Помилка: {msg.error()}")
            else:
                process_message(msg, producer)

            if time.time() - start_time > timeout:
                print(Fore.MAGENTA + "Досягнуто таймаут, зупинка споживача.")
                break

    except KeyboardInterrupt:
        print(Fore.MAGENTA + "Споживач перервано користувачем.")
    finally:
        consumer.close()
        print(Fore.GREEN + "Споживач закритий.")

if __name__ == "__main__":
    main()
