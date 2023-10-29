from confluent_kafka import Consumer, TopicPartition, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
from collections import deque


def create_topic(topic_name):
    admin_client_config = {
        "bootstrap.servers": "localhost:9092"
    }
    admin_client = AdminClient(admin_client_config)

    new_topic = NewTopic(
        topic_name,
        num_partitions=2,
        replication_factor=1
    )

    created_topic_futures = admin_client.create_topics([new_topic])

    for topic, future in created_topic_futures.items():
        try:
            future.result()
            print(f"Tema {topic} creado con éxito.")
        except Exception as e:
            print(f"Falló la creación del tema {topic}: {e}")


# Crear el tema
new_topic_name = 'Formulario-topic'
create_topic(new_topic_name)

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'grupo_consumidores',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)
consumer.subscribe([new_topic_name])

# Colas para almacenar mensajes de cada partición
queue_0 = deque()
queue_1 = deque()

# Consumir mensajes
print("Esperando mensajes...")
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # timeout en milisegundos
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(
                    f"Fin de la partición alcanzado {msg.topic()}/{msg.partition()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            if msg.partition() == 0:
                queue_0.append(msg)
            else:
                queue_1.append(msg)

        # Procesar mensajes de la partición 0 primero
        while queue_0:
            msg = queue_0.popleft()
            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode(
                'utf-8')) if msg.value() else None
            print(f"Mensaje recibido: {key} : {value}")

        # Procesar mensajes de la partición 1 si la partición 0 está vacía
        if not queue_0:
            while queue_1:
                msg = queue_1.popleft()
                key = msg.key().decode('utf-8') if msg.key() else None
                value = json.loads(msg.value().decode(
                    'utf-8')) if msg.value() else None
                print(f"Mensaje recibido: {key} : {value}")

finally:
    consumer.close()
