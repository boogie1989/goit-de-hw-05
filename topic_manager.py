from confluent_kafka.admin import AdminClient, NewTopic
from kafka_config import KAFKA_CONFIG
from colorama import Fore, Style, init

init(autoreset=True)

def create_topics(topic_names):
    admin_client = AdminClient({
        "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
        "security.protocol": KAFKA_CONFIG["security.protocol"],
        "sasl.mechanism": KAFKA_CONFIG["sasl.mechanism"],
        "sasl.username": KAFKA_CONFIG["username"],
        "sasl.password": KAFKA_CONFIG["password"],
    })

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topic_names]

    try:
        admin_client.create_topics(new_topics)
        print(Fore.GREEN + "Топіки створено успішно.")
    except Exception as e:
        print(Fore.RED + f"Сталася помилка при створенні топіків: {e}")

    existing_topics = admin_client.list_topics().topics.keys()

    print(Fore.GREEN + "\n==== Створені Топіки ====")
    for topic in topic_names:
        status = Fore.CYAN + "✅" if topic in existing_topics else Fore.RED + "❌"
        print(f"{status} {topic}")

    print(Fore.GREEN + "\n==== Всі Існуючі Топіки ====")
    for topic in existing_topics:
        if topic in topic_names:
            print(f"{Fore.YELLOW}✅ {topic} (створено)")
        else:
            print(f"{Fore.RED}ℹ️  {topic}")

if __name__ == "__main__":
    topics_to_create = ["building_sensors", "temperature_alerts", "humidity_alerts"]
    create_topics(topics_to_create)
