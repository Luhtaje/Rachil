import ssl
import time
import paho.mqtt.client as mqtt

BROKER = "broker.hivemq.com"
PORT = 8883
TOPIC = "test/esp32/retained"
MESSAGE = "hello"


def _connect_client():
    client = mqtt.Client()
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.connect(BROKER, PORT, 60)
    return client


def test_retained_message():
    """Publish a retained message and verify it is persisted on the broker."""
    # First connection publishes a retained message
    received = []

    def on_connect(client, userdata, flags, rc):
        client.subscribe(TOPIC)
        client.publish(TOPIC, MESSAGE, retain=True)

    def on_message(client, userdata, msg):
        received.append(msg.payload.decode())
        client.disconnect()

    client = _connect_client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()

    assert MESSAGE in received

    # Second connection should receive the retained message immediately
    received = []
    client = _connect_client()
    client.on_message = on_message
    client.loop_start()
    client.subscribe(TOPIC)
    time.sleep(1)
    client.loop_stop()
    client.disconnect()

    assert MESSAGE in received
