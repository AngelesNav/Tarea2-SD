from confluent_kafka import Producer
import json
import csv

def notify_stock_shortage(data):
    topic = "ingredient-topic"
    
    conf = {"bootstrap.servers": "localhost:9092"}
    producer = Producer(conf)

    producer.produce(topic, key="stock-shortage", value=json.dumps(data).encode('utf-8'))
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
        
selected_columns = ['id', 'Maestro_Motehuesillero', 'Stock'] 
data_stock = csv_to_json('data.csv', selected_columns)

#with open('stock.json', 'r') as json_file:
#    stock_shortage_data = json.load(json_file)

notify_stock_shortage(data_stock)
