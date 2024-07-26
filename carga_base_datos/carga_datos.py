'''En este código se utilizan los paquetes: os, para que Python interactúe con el sistema operativo; junto a schedule y time, para marcar el tiempo y
programar la hora de descarga. Para descargar los datos, se cargan las librerías: paramiko, que establece la conexiones SSH y FTP con el servidor; ctypes
y stat, para otorgar permisos a la carpeta seleccionada (Necesita leer y escribir información); smtplib, para iniciar sesión en el correo del usuario / enviar
el correo; por último, MIMEText y MIMEMultipart, para redactar el correo electrónico en caso de no encontrar los datos. De la misma forma, se importan los
paquetes para realizar la carga a la base de datos: numpy, parse y datetime, para manejar los tipos de datos; pandas, para manejar la estructura de los datos;
y pyodbc, para realizar la conexión con la base de datos.'''

import paramiko
import os
import ctypes
import stat
from datetime import datetime
import schedule
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import numpy as np
import pandas as pd
from dateutil.parser import parse
import pyodbc

# Se establece el envío de correo electrónico como falso por defecto
correo_enviado = False


# Se otorgan los permisos correspondientes de seguridad y acceso a la carpeta en la que se van a depositar las descargas
def otorgar_permisos(path):
    try:
        path = os.path.abspath(path)
        command = f'icacls "{path}" /grant Everyone:F'
        os.system(command)
    except Exception as e:
        print(f'No se pudieron cambiar los permisos: {e}')
        exit(1)


# Se define el procedimiento para enviar el correo electrónico
def enviar_correo():
    global correo_enviado

    if correo_enviado:
        return

    # Se definen las variables involucradas en el envio del correo electrónico
    correo_enviado = True
    remitente = 'remitente@outlook.com'
    destinatario = 'destinatario@gmail.com'
    asunto = 'Carga de datos'
    cuerpo = ('Buen día estimados.\n'
              '\n'
              'No se han cargado los datos correspondientes al día de hoy, agradecería se hiciera a la brevedad.\n'
              '\n'
              'Espero contar con su apoyo. Saludos cordiales.')

    # Se define la estructura del mensaje según MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = remitente
    msg['To'] = destinatario
    msg['Subject'] = asunto

    msg.attach(MIMEText(cuerpo, 'plain'))

    # Se introduce la información de inicio de sesión del remitente
    try:
        server = smtplib.SMTP('smtp-mail.outlook.com', 587)  # Se añade el servicio de correo a utilizar
        server.starttls()
        server.login(remitente, '********')  # Se coloca la contraseña de acceso al correo
        text = msg.as_string()
        server.sendmail(remitente, destinatario, text)  # Se envía el correo a través del servidor
        server.quit()
        print(f'Correo enviado a {destinatario}')
    except Exception as e:
        print(f'No se pudo enviar el correo: {e}')


# Se define el método de descarga de archivos
def descargar_archivos():
    global correo_enviado

    # Se define la dirección IP (enmascarada por una URL de MS Azure) que da al servidor
    hostname = 'codigo.zonacloud.azurecontainer.io'
    port = 22  # Puerto de TCP por defecto para el protocolo SSH
    usuario = 'usuario_contenedor'  # Se define el nombre de usuario para acceder
    contrasena = 'contraseñs_contenedor'  # Se define la contraseña de acceso al servidor
    fecha_actual = datetime.now()  # Se obtiene la fecha desde el sistema, se almacena en una variable
    fecha_formateada = fecha_actual.strftime(
        '%Y_%B_%d')  # Se le da el formato correcto para coincidir con la cadena de texto ruta_servidor
    ruta_servidor = f'/ruta/BI_{fecha_formateada}/'  # Se establece la ruta de los archivos dentro del servidor
    ruta_local = 'C:/ruta/Downloads/Base'  # Se establece la ruta de descarga en la computadora local

    # Se crea el directorio de la ruta local en caso de no existir
    if not os.path.exists(ruta_local):
        try:
            os.makedirs(ruta_local)
            otorgar_permisos(ruta_local)
        except OSError as e:
            print(f'Ocurrió un error al crear el directorio {ruta_local}: {e}')
            return False

    # Se define una conexión SSH para conectarse al servidor
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Se inicia la conexión SSH para conectarse al servidor
        ssh.connect(hostname, port, usuario, contrasena)
        # Se inicia el protocolo FTP
        sftp = ssh.open_sftp()

        # Se descargan de manera iterativa todos los archivos listados en la ruta del servidor
        try:
            files = sftp.listdir(ruta_servidor)
            for file in files:
                remote_file_path = os.path.join(ruta_servidor, file)
                local_file_path = os.path.join(ruta_local, file)
                sftp.get(remote_file_path, local_file_path)
                print(f'Archivo {file} descargado correctamente a {local_file_path}')
            # Se cierra la conexión con el protocolo de transferencia de archivos
            sftp.close()
            correo_enviado = False
            return True
        except IOError as e:
            print(f'La ruta en el servidor no existe: {e}')
            return False

    except Exception as e:
        print(f'Ocurrió un error: {e}')
        return False
    finally:
        # Se cierra la conexión con el servidor
        ssh.close()


# Se define el método para programar la descarga
def descarga_programada():
    intentos = 0
    while intentos < 3:  # Se establece un máximo de 3 intentos de descarga
        if descargar_archivos():
            break
        else:
            intentos += 1
            print(f'Intento {intentos} fallido. Quedan {3 - intentos} intentos')
            if intentos == 1 and not correo_enviado and datetime.now().weekday() < 5:
                enviar_correo()
            time.sleep(900)  # Se dejan espacios de 15 minutos (900 Segundos) entre reintententos de descarga


# Se crea la cadena de conexión a la base de datos; DATABASE indica la base a la que se apunta, UID el usuario y PWD la contraseña
connection_string = r'DRIVER={SQL Server};SERVER=SERViDOR\SQLEXPRESS;DATABASE=BASE_DATOS;UID=sa;PWD=CONTRASEÑA'
# Se definen las variables; carpeta, la ruta donde se encuentran almacenados los archivos; tabla, la tabla de la base de datos a la que se apunta la carga
carpeta = 'C:/ruta/Downloads/Base'
tabla = 'tabla_datos'
# Se establece un valor de fecha nula para evitar errores al momento de realizar carga
fecha_nula = datetime(1900, 1, 1)

# Se mapean las en las que no coincide el encabezado de origen y el de destino
mapeado_columnas = {
    'Num. A': 'Num# A',
    'Num. S': 'Num# S',
}


# Se define el metódo para eliminar la notación científica en los folios
def remover_notacion_cientifica(value):
    # Se evalua si el tipo de dato es una fecha, en cuyo caso lo devolverá sin cambios
    try:
        parse(value)
        return value
    except (ValueError, TypeError):
        pass
    # Se evalua si el tipo de dato es numérico de punto flotante, en cuyo caso se mostrará como un entero sin puntos decimales
    try:
        float_value = float(value)
        if not pd.isnull(float_value):
            if float_value == 0:
                return None
            elif float_value > 1e+20:
                return '{:.0f}'.format(float_value).replace('.0', '')
            else:
                return '{:.0f}'.format(float_value)
    except (ValueError, TypeError, OverflowError):
        pass

    return value


# Se define el metódo para cargar la base
def carga_base():
    # Se inicia la conexión con la base de datos
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            # Se da la instrucción de truncar la tabla a la que el script está apuntado
            cursor.execute(f"TRUNCATE TABLE {tabla}")
            conn.commit()
            print("Tabla truncada correctamente.")
            # Se establece el ciclo de carga de archivos para todos los elementos listados de la carpeta de origen
            for file_name in os.listdir(carpeta):
                if file_name.endswith('.xlsx'):
                    file_path = os.path.join(carpeta, file_name)
                    df = pd.read_excel(file_path)
                    df = df.where(pd.notnull(df), None)
                    # Se establece que las columnas que involucren fechas nulas se reemplacen por el valor asignado a la variable fecha nula
                    for column in df.select_dtypes(include=['datetime64[ns]']).columns:
                        df[column] = df[column].fillna(fecha_nula)
                    # Se establece que las columnas que involucren números decimales nulos se reemplacen por 0.0, referido al tipo de dato de la columna
                    for column in df.select_dtypes(include=[np.float64, np.float32]).columns:
                        df[column] = df[column].fillna('')
                    # Se aplica el metódo para remover la notación científica a las columnas que involucren folios
                    df['sin_notacion_1'] = df['sin_notacion_1'].apply(remover_notacion_cientifica)
                    df['sin_notacion_2'] = df['sin_notacion_2'].apply(remover_notacion_cientifica)
                    # Se aplica una conversión de tipo de dato a las columnas especificas que deben contener números en la tabla de destino
                    try:
                        df['numerico_1'] = pd.to_numeric(df['numerico_1'], errors='coerce').fillna('')
                        df['numerico_2'] = pd.to_numeric(df['numerico_2'], errors='coerce').fillna('')
                        df['numerico_3'] = pd.to_numeric(df['numerico_3'], errors='coerce').fillna('')
                    except KeyError as e:
                        print(f"Error: {e}. Columna no encontrada.")
                    # Se aplica el mapeado de columnas definido arriba
                    df.rename(columns=mapeado_columnas, inplace=True)
                    # Se define un ciclo para la inserción de filas en la tabla de destino de SQL Server
                    for index, row in df.iterrows():
                        placeholders = ', '.join(['?'] * len(row))
                        columns = ', '.join([f'[{col}]' for col in row.index])
                        sql = f"INSERT INTO {tabla} ({columns}) VALUES ({placeholders})"
                        cursor.execute(sql, tuple(row))
                    conn.commit()
                    print(f"Archivo {file_name} cargado correctamente.")
    except Exception as e:
        print(f"Error: {e}")


# Se define un mètodo que combina ambas tareas (Descarga de archivos y carga a la base de datos)
def ambas_tareas():
    descarga_programada()
    time.sleep(1800)  # Se da un espacio de 30 minutos (1800 segundos) entre tareas
    carga_base()


# Se establece la hora programada para iniciar el proceso combinado
schedule.every().day.at("02:30").do(ambas_tareas)

while True:
    schedule.run_pending()
    time.sleep(60)
