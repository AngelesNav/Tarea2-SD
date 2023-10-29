from kafka import KafkaConsumer
from itertools import zip_longest
import random

servidores_bootstrap = 'kafka:9092'
topics = ['ventas', 'stock', 'inscripcion']

# Creando grupos de consumidores para cada topic
consumer_groups = [f'grupo_consumidores_{topic}' for topic in topics]

# Creando consumidores para cada grupo
consumers = [
    KafkaConsumer(
        *topics,
        group_id=group,
        bootstrap_servers=[servidores_bootstrap]
    )
    for group in consumer_groups
]

# Partition of topics


# Crear un bucle infinito para consumir mensajes de manera alternada de cada consumidor
while True:
    for msgs in zip_longest(*consumers):
        for i, msg in enumerate(msgs):
            if msg is not None:
                print(f"Grupo de consumidores: {consumer_groups[i]}\n")
                print(f"Topic: {msg.topic}, Mensaje: {msg.value}")