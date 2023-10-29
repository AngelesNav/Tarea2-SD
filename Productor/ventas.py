from confluent_kafka import Producer
import json
import csv
def record_sale(data):
    topic = "sales-topic"
    
    conf = {"bootstrap.servers": "localhost:9092"}
    producer = Producer(conf)

    producer.produce(topic, key="sale", value=json.dumps(data).encode('utf-8'))
    producer.flush()

def csv_to_json(input_filename, output_filename):
    try:
        with open(input_filename, 'r') as csvfile:
            # Lee el archivo CSV
            csv_reader = csv.DictReader(csvfile)

            # Inicializa una lista para almacenar los objetos JSON
            json_list = []

            # Itera sobre las filas del CSV
            for row in csv_reader:
                # Agrega cada fila como un objeto JSON a la lista
                json_list.append(row)

        # Escribe la lista de objetos JSON en un archivo JSON de salida
        with open(output_filename, 'w') as jsonfile:
            json.dump(json_list, jsonfile, indent=2)

       
    except FileNotFoundError:
        print(f"El archivo {input_filename} no fue encontrado.")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

# Reemplaza 'entrada.csv' y 'salida.json' con los nombres de tus archivos reales
csv_to_json('data.csv', 'venta.json')

with open('venta.json', 'r') as json_file:
    sale_data = json.load(json_file)


record_sale(sale_data)