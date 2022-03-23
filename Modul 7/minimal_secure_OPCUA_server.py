# -*- coding: utf-8 -*-
"""
minimal_secure_OPCUA_server.py

Python Script for minimal OPC-UA Server example.
streams data from csv file.

This program is based on the FreeOpcUa package!
If necessary install FreeOpcUa via
pip install opcua 


Usage: python minimal_secure_OPCUA_server.py <filename> (optional,default:daten.csv)
"""
#import numpy as np
import sys
import pandas as pd

sys.path.insert(0, "..")
from time import sleep

try:
    from opcua import ua, Server
    from opcua.server.user_manager import UserManager
except ImportError:
   print('Error, Module opcua is required')
   print('This OPC UA Server is based on the package opcua.')
   print('Bitte installieren per Befehl:')
   print('pip install opcua')


def datavalue(column):
    '''stack overflow fix for datatype issues'''
    dv = ua.DataValue(ua.Variant(df[column][i], ua.VariantType.Double))
    dv.ServerTimestamp = None                                                
    dv.SourceTimestamp = None
    return dv

# Set User Credentials (plain text) which have access
user_db = {"PI_user": 'PI_OPCUA_password'}

# check credentials
def user_manager(isession, username, password):
    isession.user = UserManager.User
    # return true if credentials are correct
    return (username in user_db) and (password == user_db[username])

if __name__ == "__main__":
    print(__doc__)

    if (len(sys.argv) > 1):
        file = sys.argv[1]
    else:
        file = 'daten.csv'
    # Get Data für Abfragen
    df=pd.read_csv(file)

    # setup our server
    server = Server()
    server.set_endpoint("opc.tcp://localhost:4840/")
    server.set_server_name('PI_Python_OPCUA_Server')
    policyIDs = ['Username']
    server.set_security_IDs(policyIDs)
    server.user_manager.set_user_manager(user_manager)
    

    idx = 2

    # get Objects node, this is where we should put our nodes
    objects = server.get_objects_node()
    # populating our address space
    myobj = objects.add_object(idx, "Messdaten")
    a_ziel = myobj.add_variable(ua.NodeId('Application.PlcProg.aZiel_SSeg',idx), "a_ziel",0.0)
    v_ziel = myobj.add_variable(ua.NodeId('Application.PlcProg.vZiel_SSeg',idx), "v_ziel",0.0)
    Messzeit = myobj.add_variable(ua.NodeId('Application.PlcProg.tMess',idx), "Messzeit",0.0)
    Amplitude = myobj.add_variable(ua.NodeId('Application.PlcProg.amplSchwing',idx), "Amplitude",0.0)
    timestamp = myobj.add_variable(ua.NodeId('Application.PlcProg.sysTimeNs',idx), "timestamp",0.0)
    ActPosition = myobj.add_variable(ua.NodeId('Application.PlcProg.rActualPosition',idx), "ActPosition",0.0)
    ActVelocity = myobj.add_variable(ua.NodeId('Application.PlcProg.rActualVelocity',idx), "ActVelocity",0.0)
    ActTorqueForce = myobj.add_variable(ua.NodeId('Application.PlcProg.rActualTorqueForce',idx), "ActTorqueForce",0.0)
    rActualTorqueCurrent = myobj.add_variable(ua.NodeId('Application.PlcProg.rActualTorqueCurrent',idx), "rActualTorqueCurrent", 0.0)
    bActivateDemo = myobj.add_variable(ua.NodeId('Application.PlcProg.bActivateDemo',idx), "bActivateDemo",0.0)
    # Set MyVariable to be writable by clients
    a_ziel.set_writable()  
    v_ziel.set_writable()
    Messzeit.set_writable()
    Amplitude.set_writable()
    timestamp.set_writable()
    ActPosition.set_writable()
    ActVelocity.set_writable()
    ActTorqueForce.set_writable()
    bActivateDemo.set_writable()
    rActualTorqueCurrent.set_writable()
    
       
    # starting!
    server.start()
    print('Server aktiv')

    # Erzeuge Werte in Knoten falls Abfrage kommt
    try:
        recent=0  
        bActivateDemo.set_value(1)
        while True:
            for i in range(len(df['run'])):
                if(recent!=df['run'][i]):
                    bActivateDemo.set_value(0)
                    sleep(0.4)
                    bActivateDemo.set_value(1)
                else:
                    sleep(0.02)  
                a_ziel.set_value(datavalue('a_ziel'))
                v_ziel.set_value(datavalue('v_ziel'))
                Messzeit.set_value(df['Messzeitms'][i]*1e6)
                Amplitude.set_value(datavalue('Amplitude'))
                timestamp.set_value(datavalue('timestamp'))
                ActPosition.set_value(df['Position'][i])
                ActVelocity.set_value(df['Velocity'][i])
                ActTorqueForce.set_value(df['TorqueForce'][i])   
                rActualTorqueCurrent.set_value(df['TorqueGeneratingCurrent'][i])
                recent=df['run'][i]
              
                
    finally:
        # close connection, remove subcsriptions, etc
        server.stop()
        print('Server beendet')
