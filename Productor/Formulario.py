from confluent_kafka import Producer
import json
import csv

def send_registration_form(is_paid, data):
    topic = "Formulario-topic"
    if is_paid:
        topic = "priority-registration-topic"

    conf = {"bootstrap.servers": "localhost:9092"}
    producer = Producer(conf)

    producer.produce(topic, key="registration", value=data)
    producer.flush()

def csv_to_json(input_filename, output_filename):
    try:
        with open(input_filename, 'r') as csvfile:
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

csv_to_json('data3.csv', 'Manifest.json')

with open('Manifest.json', 'r') as json_file:
    registration_data = json.load(json_file)

is_paid = registration_data[0].get("Paid", False)

send_registration_form(is_paid, registration_data)

#registration_data = {"name": "Nombre","apellido": "apellido", "email": "correo@ejemplo.com", "contraseña": "contraseña", "paid": "paid"}
#Stock - la ganancia seria random -> en base a esto generar las 3 datas que se requieren para los productores

