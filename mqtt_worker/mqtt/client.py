import os

import paho.mqtt.client as mqtt


def create_client() -> mqtt.Client:
    client_id = os.getenv("MQTT_CLIENT_ID", "siwatt-worker")
    client = mqtt.Client(client_id=client_id, clean_session=True)
    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    if username:
        client.username_pw_set(username, password)
    return client