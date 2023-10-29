from confluent_kafka import Producer
import json
import csv

def notify_stock_shortage(data):
    topic = "ingredient-topic"
    
    conf = {"bootstrap.servers": "localhost:9092"}
    producer = Producer(conf)

    producer.produce(topic, key="stock-shortage", value=json.dumps(data).encode('utf-8'))
    producer.flush()
def csv_to_json(input_filename, output_filename):
    try:
        with open(input_filename, 'r') as csvfile:
            # Lee el archivo CSV
            csv_reader = csv.DictReader(csvfile)
            json_list = []

            for row in csv_reader:
                json_list.append(row)

        with open(output_filename, 'w') as jsonfile:
            json.dump(json_list, jsonfile, indent=2)

       
    except FileNotFoundError:
        print(f"El archivo {input_filename} no fue encontrado.")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

csv_to_json('data2.csv', 'stock.json')

with open('stock.json', 'r') as json_file:
    stock_shortage_data = json.load(json_file)

notify_stock_shortage(stock_shortage_data)
