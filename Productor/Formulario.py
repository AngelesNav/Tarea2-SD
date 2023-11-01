from confluent_kafka import Producer
import json
import csv


def send_registration_form(data_list):
    topic = "Formulario-topic"
    conf = {"bootstrap.servers": "localhost:9092"}
    producer = Producer(conf)

    for data in data_list:
        is_paid = data.get("Paid", "False").lower() == "true"
        key = "paid" if is_paid else "not_paid"
        producer.produce(topic, key=key, value=json.dumps(data))
        print(f'Message sent: {data}')

    producer.flush()


def csv_to_json(input_filename):
    try:
        with open(input_filename, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            json_list = [row for row in csv_reader]
        return json_list
    except FileNotFoundError:
        print(f"El archivo {input_filename} no fue encontrado.")
        return []
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return []


data_list = csv_to_json('data.csv')
send_registration_form(data_list)
