# -*- coding:utf-8 -*-
import time
import logging
from queue import Queue as ThreadQueue
from threading import Thread, RLock

from kombu import Connection, Exchange, Queue, Consumer, Producer

from .utils import to_unicode

logger = logging.getLogger("clientmq")

class Msg(object):
    def __init__(self, nodeName, message):
        self.nodeName = nodeName
        self.message = message


class ServerConsumer(object):
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
            queue = Queue(name=self.queueName, durable=False)
            queue.queue_declare(channel=channel)
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


class ServerProducer(object):
    connection = None
    channel = None

    def __init__(self, connection):
        self.connection = connection
        self.lock = RLock()

        self.connect()

    def connect(self):
        self.close_channel()

        try:
            self.channel = self.connection.channel()
            exchange = Exchange(name='policymq.direct', type='direct', durable=False)
            exchange.declare(channel=self.channel)
            self.producer = Producer(channel=self.channel)
        except Exception as e:
            logger.exception(e)
            pass

    def close_channel(self):
        try:
            if self.channel:
                self.channel.close()
        except Exception as e:
            pass

    def publish(self, message, routing_key):
        try:
            self.lock.acquire()
            properties = {"timestamp": int(time.time())}
            self.producer.publish(body=message, routing_key=routing_key, **properties)
            return True
        except Exception as e:
            logger.exception(e)
            time.sleep(1)
            self.connect()
            return False
        finally:
            self.lock.release()


class ServerMQ(object):
    connection = None
    callback = None
    consumers = []

    def __init__(self, host, vhost,  callback=None, threadsize=1):

        self.init_connect(host, vhost)
        self.callback = callback
        self.queue = ThreadQueue()
        self.producer = ServerProducer(self.connection)
        self.initRecv(threadsize)
        self.startThread()

    def init_connect(self, host, vhost):
        self.connection = Connection(hostname=host, virtual_host=vhost, userid='admin', password='lua12378900')

    def initRecv(self, threadsize):
        for index in range(threadsize):
            consumer = ServerConsumer(self.connection, 'HOST', self.callback)
            while not consumer.init():
                time.sleep(1)
            self.consumers.append(consumer)

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
                    while not self.producer.publish(msg.message, msg.nodeName):
                        time.sleep(1)
                except Exception as e:
                    logger.exception(e)
                    pass

        t = Thread(target=start_produce)
        t.start()

    def update_bean(self, cls_name, content, node_name=None):
        msg = '1' + to_unicode(cls_name) + ':' + to_unicode(content)
        if node_name:
            self.queue.put(Msg(node_name, msg))
        else:
            self.queue.put(Msg("ALL_", msg))

    def delete_bean(self, cls_name, content, node_name=None):
        msg = '0' + to_unicode(cls_name) + ':' + to_unicode(content)
        if node_name:
            self.queue.put(Msg(node_name, msg))
        else:
            self.queue.put(Msg("ALL_", msg))

    def is_open(self):
        if self.connection:
            return self.connection.connected
        else:
            return False


