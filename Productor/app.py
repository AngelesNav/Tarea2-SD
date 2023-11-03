import csv
import sys
from email.message import EmailMessage
import ssl
import smtplib
import json

data = []

def agregar_al_csv(archivo_csv, datos):

    with open(archivo_csv, 'a', newline='') as csvfile:

        campos = ['Nombre', 'Apellido', 'email', 'contraseÃ±a', 'Paid']


        escritor_csv = csv.DictWriter(csvfile, fieldnames=campos)


        if csvfile.tell() == 0:
            escritor_csv.writeheader()


        escritor_csv.writerow(datos)

def enviar_correo(file_csv):
    nombre = input("Ingrese su nombre: ")
    apellido = input("Ingrese su apellido: ")
    correo = input("Ingrese su correo: ")
    contrasena = input("Ingrese su contraseña: ")
    pago = input("¿Ha pagado? (true/false): ")


    nueva_fila = {'Nombre': nombre, 'Apellido': apellido, 'email': correo, 'contraseÃ±a': contrasena, 'Paid': pago}


    agregar_al_csv(file_csv, nueva_fila)

    print("Su formulario se ha enviado correctamente.")


    email_sender = correo
    subject = "Envio de formulario"
    body = f"""
        Su cuenta se ha creado con exito.

        Su clave es: {contrasena}
    """

    body1 = json.dumps(body)
    email_reciever = "sebastian.cornejo_i@mail.udp.cl"

    em = EmailMessage()
    em["From"] = email_sender
    em["To"] = email_reciever
    em["Subject"] = subject
    em.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context = context) as smtp:
        smtp.login(email_sender,contrasena)
        smtp.sendmail(email_sender,email_reciever, em.as_string())
