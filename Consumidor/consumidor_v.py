from kafka import KafkaConsumer
from itertools import zip_longest
import json

servidores_bootstrap = 'localhost:9092'
topics = ['sales-topic']

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

while True:
    for msgs in zip_longest(*consumers):
        for i, msg in enumerate(msgs):
            if msg is not None:
                try:
                    messages = json.loads(msg.value.decode('utf-8'))
                    if isinstance(messages, list):
                        for message in messages:
                            print(f"Mensaje recibido: {message}")
                            # Verifica si el stock es igual a 0 -->No se logro :(
                            #if 'Stock' in message and message['Stock'] == 0:
                                # Genera un mensaje por la terminal
                            #    print(f"------------------------")
                            #    maestro_id = message.get('id', 'ID no disponible')
                            #    print(f"¡El stock es 0 para el maestro de motehuesillero con ID: {maestro_id}!")
                            #    print(f"Se le entrego un nuevo stock")
                            #    print(f"------------------------")
                    else:
                        print(f"Mensaje no es una lista válida: {messages}")
                except json.JSONDecodeError as e:
                    print(f"Error al decodificar JSON: {e}")
