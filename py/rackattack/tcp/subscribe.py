import threading
import logging
import pika
import simplejson
from rackattack.tcp import suicide
from rackattack.tcp import publish
from rackattack import pikapatchwakeupfromanotherthread

# handler = logging.StreamHandler()
# _logger = logging.getLogger(__name__)
# _logger.addHandler(handler)
# _logger.setLevel(logging.INFO)


class Subscribe(threading.Thread):
    def __init__(self, amqpURL, skipSuicide=False):
        self._connection = None
        self._channel = None
        self._amqpURL = amqpURL
        self._registered = dict()
        self._skipSuicide = skipSuicide
        self._readyEvent = threading.Event()
        self._closed = False
        threading.Thread.__init__(self)
        self.daemon = True
        logging.debug("Creating a channel to Rackattack's RabbitMQ...")
        threading.Thread.start(self)
        self._readyEvent.wait()
        logging.debug("Channel is open.")

    def registerForInagurator(self, id, callback):
        exchange = self._exchangeForInaugurator(id)
        self._wakeUpFromAnotherThread.runInThread(self._registerToExchange,
                                                  exchange=exchange,
                                                  onRegisteredCallback=callback)

    def unregisterForInaugurator(self, id):
        exchange = self._exchangeForInaugurator(id)
        self._wakeUpFromAnotherThread.runInThread(self._unregisterFromExchange,
                                                  exchange=exchange)

    def registerForAllocation(self, id, callback):
        exchange = publish.Publish.allocationExchange(id)
        self._wakeUpFromAnotherThread.runInThread(self._registerToExchange,
                                                  exchange=exchange,
                                                  onRegisteredCallback=callback)

    def unregisterForAllocation(self, id):
        exchange = publish.Publish.allocationExchange(id)
        self._wakeUpFromAnotherThread.runInThread(self._unregisterFromExchange,
                                                  exchange=exchange)

    def registerForAllAllocations(self, callback):
        exchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        self._wakeUpFromAnotherThread.runInThread(self._registerToExchange,
                                                  exchange=exchange,
                                                  onRegisteredCallback=callback)

    def unregisterForAllAllocations(self):
        exchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        self._wakeUpFromAnotherThread.runInThread(self._unregisterFromExchange, exchange=exchange)

    def close(self):
        logging.debug("Closing connection")
        self._closed = True
        self._channel.close()
        self._connection.close()

    def _registerToExchange(self, exchange, onRegisteredCallback):
        assert exchange not in self._registered
        _Subscribe(self._channel, exchange, self._registered, onRegisteredCallback)

    def _unregisterFromExchange(self, exchange):
        assert exchange in self._registered
        self._channel.basic_cancel(lambda *args: None, self._registered[exchange])
        del self._registered[exchange]

    def _exchangeForInaugurator(self, id):
        return "inaugurator_status__%s" % id

    def _onConnectionClosed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closed:
            self._connection.ioloop.stop()
        elif self._skipSuicide:
            logging.error("Connection closed, not commiting suicide: %(replyCode)s %(replyText)s", dict(
                replyCode=reply_code, replyText=reply_text))
        else:
            logging.error("Connection closed, committing suicide: %(replyCode)s %(replyText)s", dict(
                replyCode=reply_code, replyText=reply_text))
            suicide.killSelf()

    def _onConnectionOpen(self, unused_connection):
        logging.debug('Connection is open. Openning a channel...')
        self._connection.add_on_close_callback(self._onConnectionClosed)
        self._connection.channel(on_open_callback=self._onChannelOpen)

    def _onChannelClosed(self, channel, reply_code, reply_text):
        logging.error('Channel %(channel)i was closed: (%(replyCode)s) %(replyText)s', dict(
            channel=channel, replyCode=reply_code, replyText=reply_text))
        self._connection.close()

    def _onChannelOpen(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._onChannelClosed)
        self._readyEvent.set()

    def run(self):
        logging.debug("Connecting to '%(amqpURL)s'...", dict(amqpURL=self._amqpURL))
        try:
            self._connection = pika.SelectConnection(
                pika.URLParameters(self._amqpURL),
                self._onConnectionOpen,
                stop_ioloop_on_close=False)
        except Exception as ex:
            logging.exception("Subscribe thread has crashed: %(message)s", dict(message=str(ex)))
            if not self._skipSuicide:
                logging.debug("Commiting suicide...")
                suicide.killSelf()
            raise
        self._wakeUpFromAnotherThread = \
            pikapatchwakeupfromanotherthread.PikaPatchWakeUpFromAnotherThread(logging, self._connection)
        logging.debug("Starting pika's IO loop...")
        self._connection.ioloop.start()


class _Subscribe:
    def __init__(self, channel, exchangeName, registered, callback):
        self._channel = channel
        self._exchangeName = exchangeName
        self._registered = registered
        self._callback = callback
        self._channel.exchange_declare(self._onExchangeDeclared, self._exchangeName, 'fanout')

    def _onMessage(self, channel, basicDeliver, properties, body):
        self._channel.basic_ack(basicDeliver.delivery_tag)
        self._callback(simplejson.loads(body))

    def _onBind(self, *args):
        self._channel.basic_consume(self._onMessage, self._queueName, consumer_tag=self._consumerTag)
        logging.debug("Listening on exchange %(exchange)s", dict(exchange=self._exchangeName))

    def _onQueueDeclared(self, methodFrame):
        self._queueName = methodFrame.method.queue
        self._consumerTag = self._queueName + "__consumer"
        self._registered[self._exchangeName] = self._consumerTag
        self._channel.queue_bind(self._onBind, self._queueName, self._exchangeName, routing_key='')

    def _onExchangeDeclared(self, frame):
        self._channel.queue_declare(callback=self._onQueueDeclared, exclusive=True)
