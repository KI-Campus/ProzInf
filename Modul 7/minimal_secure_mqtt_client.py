# -*- coding: utf-8 -*-
"""
minimal_secure_mqtt_client.py

Python Script for minimal MQTT Client that streams data from csv file.

This program is based on the paho.mqtt package!
If necessary install paho.mqtt via
pip install paho-mqtt 

Usage: python minimal_secure_mqtt_client.py <filename> (optional, default:Sensor_Data.csv)
"""

# import required packages

import json
import pandas as pd
import random
from time import sleep
try:
    from paho.mqtt import client as mqtt_client
except ImportError:
   print('Error, Module ModuleName is required')
   print('This MQTT Client is based on the package paho.mqtt.')
   print('Bitte installieren per Befehl:')
   print('pip install paho-mqtt')

# Set broker
broker = 'localhost'
port = 1883
topic = 'PI/Python-Sensor/Daten'
status_topic = 'PI/Python-Sensor/Status'
client_id = f'PI_mqtt_{random.randint(0, 999)}'
username = 'PI_MQTT_Publisher'
password = 'pub_password'

# Get Data for publishing
data = pd.read_csv('Sensor_Data.csv')

# Function for Connecting with Broker
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print('Verbindung mit MQTT Broker ' + broker + ' erfolgreich!')        
        else:
            print('Verbindung zu MQTT Broker Fehlgeschlagen!\n Return Code %d\n', rc)
    
    # Erstelle Client
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    
    # Einstellung last will and testament
    last_will = 'Sensor Fehler'
    client.will_set(status_topic, last_will, qos=1, retain=True)
    
    client.on_connect = on_connect
    client.connect(broker, port)
    client.publish(status_topic, 'Sensor online', retain=True)
    return client

# Function for publishing data to Broker
def publish(client):
    while True:
        for i in range(len(data['Acc_X'])):
            sleep(0.1) 
            msg_dict = {"Acc_x": data.loc[i, 'Acc_X'],
                        "Acc_y": data.loc[i, 'Acc_Y']}
            msg=json.dumps(msg_dict)
            client.publish(topic, msg)

if __name__ == "__main__":
    # Client verbinden    
    client = connect_mqtt()
    try:
        client.loop_start()
        publish(client)
    finally:
        # Client Verbindung trennen
        client.publish(status_topic, 'Sensor offline', retain=True)
        client.loop_stop()
        client.disconnect() 
        print('Verbindung mit MQTT Broker beendet!')