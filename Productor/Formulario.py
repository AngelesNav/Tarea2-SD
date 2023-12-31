from confluent_kafka import Producer
import json
import csv
import app

filecsv = "data3.csv"

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
        return []



app.enviar_correo(filecsv)
selected_columns = ['Nombre', 'apellido', 'Paid']
data_list = csv_to_json(filecsv, selected_columns)
send_registration_form(data_list)
