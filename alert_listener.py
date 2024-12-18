import json
from confluent_kafka import Consumer
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
        "group.id": "alert_group",
        "auto.offset.reset": "earliest",
    })

def main():
    consumer = create_consumer()
    consumer.subscribe(["temperature_alerts", "humidity_alerts"])

    print(Fore.BLUE + "Запущено споживача сповіщень...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                print(Fore.RED + f"Помилка: {msg.error()}")
            else:
                alert = json.loads(msg.value().decode("utf-8"))
                print(Fore.MAGENTA + f"Отримано сповіщення: {alert['message']} для датчика {alert['sensor_id']}")

    except KeyboardInterrupt:
        print(Fore.MAGENTA + "Споживач сповіщень перервано.")
    finally:
        consumer.close()
        print(Fore.GREEN + "Споживач сповіщень закритий.")

if __name__ == "__main__":
    main()
