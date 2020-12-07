import socketio
import eventlet
import pika
import requests
import json

URL = "http://localhost:15672/api/queues"

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)


@sio.event
def create_queue(sid, queue_name):
    print("creating ", queue_name)
    channel.queue_declare(queue=queue_name)
    pass


@sio.event
def delete_queue(sid, queue_name):
    print("deleting", queue_name)
    channel.queue_delete(queue=queue_name)

    pass


@sio.event
def create_topic(sid, topic_name):
    print(topic_name)
    channel.exchange_declare(exchange=topic_name, exchange_type='topic')
    pass


@sio.event
def hear_messages(sid, queue_name):
    channel.basic_consume(queue=queue_name,
                          auto_ack=True,
                          on_message_callback=callback)

    channel.start_consuming()


@sio.event
def send_message(sid, data):
    channel.basic_publish(exchange='',
                          routing_key=data["queue_name"],
                          body=data["message"])
    print(data["message"])
    pass


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


# TODO include messages quantity
@sio.event
def get_queues(sid):
    data = requests.get(URL, auth=('guest', 'guest'))
    result = json.loads(data.content.decode('utf8'))

    queues = [queue["name"] for queue in result]
    return queues


@sio.event
def connect(sid, environ):
    print('Cliente conectado ', sid)


if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('', 4113)), app)
