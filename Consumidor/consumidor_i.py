from kafka import KafkaConsumer
from itertools import zip_longest
import json

servidores_bootstrap = 'localhost:9092'   
topics = ['ingredient-topic']

# Creando grupos de consumidores para cada topic
consumer_groups = [f'grupo_consumidores_{topic}' for topic in topics]
consumers = [
    KafkaConsumer(
        *topics,
        group_id=group,
        bootstrap_servers=[servidores_bootstrap]
    )
    for group in consumer_groups
]

while True:
    for msgs in zip_longest(*consumers):
        for i, msg in enumerate(msgs):
            if msg is not None:
                print(f"Grupo de consumidores: {consumer_groups[i]}")
                try:
                    messages = json.loads(msg.value.decode('utf-8'))
                    for message in messages:
                        print(f"Mensaje recibido: {message}")
                except json.JSONDecodeError as e:
                    print(f"Error al decodificar JSON: {e}")
