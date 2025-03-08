import pika
import json
from typing import Callable


class RPCServer:
    def __init__(self, host: str, queue: str, func: Callable):
        self.host = host
        self.__func = func
        self.queue = queue

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)

    def on_request(self, ch, method, props, body):
        # NOTE: There should be a reason for line #21
        body_dict = json.loads(json.loads(body))
        response = self.__func(**body_dict)

        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id
            ),
            body=str(response)
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.on_request)
        self.channel.start_consuming()
    
    def stop_consuming(self):
        self.channel.stop_consuming()

