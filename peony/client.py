# -*- coding:utf-8 -*-
import logging
import time
from queue import Queue as ThreadQueue
from threading import Thread, RLock

from kombu import Connection, Exchange, Queue, Consumer, Producer

from .utils import to_unicode

logger = logging.getLogger("clientmq")


class ClientConsumer(object):
    connection = None
    queueName = None
    callback = None

    def __init__(self, connection, queueName, callback):
        self.connection = connection
        self.queueName = queueName
        self.callback = callback

    def init(self):
        try:
            self.channel = channel = self.connection.channel()
            exchange = Exchange(name='policymq.direct', type='direct', durable=False)
            exchange.declare(channel=channel)
            queue = Queue(name=self.queueName, durable=False)
            queue.queue_declare(channel=channel)
            queue.bind_to(exchange=exchange, routing_key='ALL_', channel=channel)
            queue.bind_to(exchange=exchange, routing_key=self.queueName, channel=channel)
            consumer = Consumer(channel=channel, queues=[queue], callbacks=[self._consume])
            consumer.consume()
            return True
        except Exception as e:
            logger.exception(e)
            return False

    def _consume(self, body, message):
        if self.callback and callable(self.callback):
            timestamp = message.properties.get('timestamp', int(time.time()))
            self.callback(body, timestamp)
        message.ack()


class ClientProducer(object):
    connection = None
    queueName = None
    channel = None

    def __init__(self, connection, queueName):
        self.connection = connection
        self.queueName = queueName
        self.lock = RLock()

        self.connect()

    def connect(self):
        self.close_channel()

        try:
            self.channel = self.connection.channel()
            queue = Queue(name=self.queueName, durable=False)
            queue.declare(channel=self.channel)
            self.producer = Producer(channel=self.channel, routing_key='HOST')
        except Exception as e:
            logger.exception(e)

    def close_channel(self):
        try:
            if self.channel:
                self.channel.close()
        except Exception as e:
            logger.exception(e)

    def publish(self, message):
        try:
            self.lock.acquire()
            properties = {"timestamp": int(time.time())}
            self.producer.publish(body=message, **properties)
            return True
        except Exception as e:
            logger.exception(e)
            time.sleep(1)
            self.connect()
            return False
        finally:
            self.lock.release()


class ClientMQ(object):
    connection = None
    nodeName = None
    callback = None

    def __init__(self, host, vhost, username, password,  nodeName, callback):
        self.init_connect(host, vhost, username, password)
        self.nodeName = nodeName
        self.callback = callback
        self.queue = ThreadQueue()
        self.produder = ClientProducer(self.connection, 'HOST')
        self.initRecv()

        self.startThread()

    def init_connect(self, host, vhost, username, password):
        self.connection = Connection(hostname=host, virtual_host=vhost, userid=username, password=password)

    def initRecv(self):
        self.consumer = ClientConsumer(self.connection, self.nodeName, self.callback)
        while not self.consumer.init():
            time.sleep(1)

        def start_consume():
            while True:
                self.connection.drain_events()

        t = Thread(target=start_consume)
        t.start()

    def startThread(self):
        def start_produce():
            while True:
                try:
                    msg = self.queue.get()
                    while not self.produder.publish(message=msg):
                        time.sleep(1)
                except Exception as e:
                    logger.exception(e)

        t = Thread(target=start_produce)
        t.start()

    def is_open(self):
        if self.connection:
            return self.connection.connected
        else:
            return False

    def update_bean(self, cls_name, content):
        msg = '1' + to_unicode(cls_name) + ':' + to_unicode(content)
        self.queue.put(msg)

    def delete_bean(self, cls_name, ids):
        msg = '0' + to_unicode(cls_name) + ':' + to_unicode(ids)
        self.queue.put(msg)
