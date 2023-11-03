import random
from confluent_kafka import Producer
import json
import csv

def record_sale(data):
    topic = "sales-topic"
    
    conf = {"bootstrap.servers": "localhost:9092"}
    producer = Producer(conf)

    producer.produce(topic, key="sale", value=json.dumps(data).encode('utf-8'))
    producer.flush()

def csv_to_json(input_filename, selected_columns):
    try:
        with open(input_filename, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            json_list = [row for row in csv_reader]

        # Seleccionar solo las columnas especificadas
        selected_data_list = []
        for row in json_list:
            selected_data = {col: row[col] for col in selected_columns}
            selected_data_list.append(selected_data)

        return selected_data_list
    except FileNotFoundError:
        print(f"El archivo {input_filename} no fue encontrado.")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

def restar_dos_a_stock(archivo_entrada, archivo_salida):
    with open(archivo_entrada, 'r', newline='') as entrada_csv:
        reader = csv.reader(entrada_csv)
        filas_modificadas = []

        for fila in reader:
            if not fila:
                continue

            # Realiza la operación de resta en la columna "Stock"
            try:
                stock = int(fila[-1])
                if stock == 0 or stock<0:
                    stock = random.randint(1, 19)  # Genera un nuevo stock aleatorio entre 1 y 19
                else:
                    stock -= 2
                fila[-1] = str(stock)
            except ValueError:
                pass
            filas_modificadas.append(fila)

    with open(archivo_salida, 'w', newline='') as salida_csv:
        writer = csv.writer(salida_csv)
        writer.writerows(filas_modificadas)

# Uso de la función para restar 2 al valor de "Stock" en un archivo CSV
archivo_entrada = 'data.csv'
archivo_salida = 'data.csv'

restar_dos_a_stock(archivo_entrada, archivo_salida)
selected_columns = ['id', 'Maestro_Motehuesillero', 'ganancia'] # antes aca se habria agregado el stock
data_venta = csv_to_json('data.csv', selected_columns)          # para la alerta
record_sale(data_venta)
