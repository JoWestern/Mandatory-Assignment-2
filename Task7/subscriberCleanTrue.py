import random
import time
from paho.mqtt import client as mqtt_client

broker = 'broker.emqx.io'
port = 1883
topic = "Sensor/Temp"
# Generate a Client ID with the subscribe prefix.
client_id = 'subscriber1'
# username = 'emqx'
# password = 'public'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Multi-Level Subscriber connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id, clean_session=True)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    def on_subscribe(client, userdata, mid, granted_qos):
        print("Subscribed to topic with QoS levels:", granted_qos)
        time.sleep(5)  # Disconnect after 5 seconds
        client.disconnect()

    def on_disconnect(client, userdata, rc):
        print("Disconnected from MQTT Broker!")
        time.sleep(30)  # Wait for 30 seconds before reconnecting
        client.connect(broker, port)
        client.subscribe(topic, qos=1)  # Re-subscribe after reconnecting

    client.subscribe(topic, qos=1)
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()
