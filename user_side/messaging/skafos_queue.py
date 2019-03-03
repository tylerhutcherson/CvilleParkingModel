import pika
import threading
from .exceptions import QueueError
import logging
from time import sleep
from queue import *

logger = logging.getLogger(__name__)


class QueueError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class SkafosQueue(object):
    def __init__(self):
        self.exchange_name = 'default'
        self.consume_queue = None
        self.consumer_connection = None
        self.stop_consumer = None
        self.publisher_queue = None
        self.publisher_connection = None
        self.stop_publisher = None
        self.broker_name = "internal-a32f6d0b23d6e11e9b93c02d86058a9b-674234305.us-east-1.elb.amazonaws.com"
        self.broker_port = "5672"
        self.vhost = ""
        self.broker_un = "metismachine"
        self.broker_pw = "skafosqueue"
        self.broker_url = f'amqp://{self.broker_un}:{self.broker_pw}@{self.broker_name}:{self.broker_port}/{self.vhost}?heartbeat=0&retry_delay=10&connection_attempts=20'
        self.parameters = pika.URLParameters(self.broker_url)

    def consumer_connect(self):
        self.consumer_connection = pika.BlockingConnection(self.parameters)
        self.consumer_channel = self.consumer_connection.channel()

    def publisher_connect(self):
        self.publisher_connection = pika.BlockingConnection(self.parameters)
        self.publisher_channel = self.publisher_connection.channel()

    def _build_queues(self, queue_names, routing_keys, delete):
        # Setup queues with bindings on the exchange
        if isinstance(queue_names, list) & isinstance(routing_keys, list) & (len(queue_names) == len(routing_keys)):
            for name in queue_names:
                if isinstance(name, str):
                    if delete:
                        self.publisher_channel.queue_delete(queue=name)
                    self.publisher_channel.queue_declare(queue=name,
                                                         durable=True,
                                                         exclusive=False,
                                                         auto_delete=False)
                else:
                    raise QueueError('Must pass valid names for Skafos Queues.')
        elif isinstance(queue_names, str) & isinstance(routing_keys, str):
            if delete:
                self.publisher_channel.queue_delete(queue=queue_names)
            self.publisher_channel.queue_declare(queue=queue_names,
                                                 durable=True,
                                                 exclusive=False,
                                                 auto_delete=False)
        else:
            raise QueueError('Must pass either a list or single string for queue names and routing keys.')

    def _build_exchange(self, exchange_name=None):
        if not exchange_name:
            exchange_name = self.exchange_name
        self.publisher_channel.confirm_delivery()
        self.publisher_channel.exchange_declare(exchange=exchange_name,
                                                exchange_type='topic',
                                                durable=True)

    def _bind_queues(self, queue_names, routing_keys):
        # Setup queues with bindings on the exchange
        if isinstance(queue_names, list) & isinstance(routing_keys, list) & (len(queue_names) == len(routing_keys)):
            for name, key in zip(queue_names, routing_keys):
                if isinstance(key, str):
                    self.publisher_channel.queue_bind(exchange=self.exchange_name,
                                                      queue=name,
                                                      routing_key=key)
                else:
                    raise QueueError('Must pass valid routing keys for Skafos Queues.')
        elif isinstance(queue_names, str) & isinstance(routing_keys, str):
            self.publisher_channel.queue_bind(exchange=self.exchange_name,
                                              queue=queue_names,
                                              routing_key=routing_keys)
        else:
            raise QueueError('Must pass either eqaul length lists or single strings for queue names and routing keys.')

    def __setup(self):
        queue_params = self.publisher_queue.get(block=True)
        try:
            self._build_queues(queue_names=queue_params.get('queue_names'),
                               routing_keys=queue_params.get('routing_keys'),
                               delete=queue_params.get('delete'))
            self._build_exchange(exchange_name=queue_params.get('exchange_name'))
            self._bind_queues(queue_names=queue_params.get('queue_names'),
                              routing_keys=queue_params.get('routing_keys'))
            self.publisher_results.put((True, queue_params))
        except Exception as e:
            logger.error(f'Setup ERROR = {e}')
            self.publisher_results.put((False, e))

    def setup(self, queue_names, routing_keys, delete=False, exchange_name=None):
        """Build a single, or multiple queue instances by providing new names and routing keys
           to bind on an exchange. Once created, they are made available for publishing and
           consuming - in and between Jobs.

           Parameters
           ----------
           queue_names : list or str
               Names of the individual queues to create. Can be a single queue or a list.
           routing_keys : list or str
               Keys to use for each queue created above. Can be a single key or a list.
           delete : boolean
               Whether or not to blow away queues with the same name prior to creating. Defaults to False.
           exchange_name : str
               Name of the exchange to publish to. Defaults to "default".               

            Returns
            -------
            status : boolean
                Whether or not the setup operation was successful.
        """
        logger.info(f'Setting up Skafos Queue on Exchange: {self.exchange_name}')
        try:
            if not self.publisher_queue:
                self._start_publishing()
            if (exchange_name is not None) & isinstance(exchange_name, str):
              self.exchange_name = exchange_name
            self.publisher_queue.put({
                'queue_names': queue_names,
                'routing_keys': routing_keys,
                'delete': delete,
                'exchange_name': self.exchange_name})
            self.publisher_connection.add_callback_threadsafe(lambda: self.__setup())
            status, msg = self.publisher_results.get(block=True)
            if not status:
                raise msg
            return status
        except Exception as e:
            raise e

    def run_consumer(self, *args):
        stop_event = args[0]
        try:
            self.consumer_connect()
            self.consumer_channel.basic_qos(prefetch_count=1)
        except Exception as e:
            logger.error(f'Connection ERROR = {e}')
        logger.info(f'stopevent is set = {stop_event.is_set()}')
        while (not stop_event.is_set()):
            self.consumer_connection.process_data_events()
            self.consumer_connection.sleep(2.0)
            # self.stop_event.wait(2.0)
        if self.consumer_channel.is_open:
            self.consumer_channel.close()
        self.consumer_connection.close()

    def run_publisher(self, *args):
        stop_event = args[0]
        try:
            self.publisher_connect()
            self.publisher_channel.basic_qos(prefetch_count=1)
        except Exception as e:
            logger.error(f'Connection ERROR = {e}')
        logger.info(f'stopevent is set = {stop_event.is_set()}')
        while (not stop_event.is_set()):
            self.publisher_connection.process_data_events()
            self.publisher_connection.sleep(2.0)
            # self.stop_event.wait(2.0)
        if self.publisher_channel.is_open:
            self.publisher_channel.close()
        self.publisher_connection.close()

    def __ack(self, delivery_tag):
        try:
            self.consumer_channel.basic_ack(delivery_tag=delivery_tag)
        except Exception as e:
            logger.error(f'Ack ERROR = {e}')

    def ack(self, delivery_tag):
        """When sent by the client, this method acknowledges one message from the queue (safe removal).
           Parameters
           ----------
           delivery_tag: int
               Server assigned delivery tag. Use method.delivery_tag.

        """
        self.consumer_connection.add_callback_threadsafe(lambda: self.__ack(delivery_tag=delivery_tag))
        self.consume_queue.task_done()

    def nack(self, delivery_tag):
        self.consumer_connection.add_callback_threadsafe(lambda: self.consumer_channel.basic_nack(delivery_tag=delivery_tag))
        self.consume_queue.task_done()

    def stop(self):
        """Close all publishing and consuming connections to the active queues."""
        logger.info("Stop Run")
        if self.stop_publisher is not None:
            self.stop_publisher.set()
        if self.stop_consumer is not None:
            self.stop_consumer.set()

    def __publish(self):
        publish_request = self.publisher_queue.get(block=True)
        try:
            self.publisher_channel.publish(exchange=publish_request.get('exchange_name'),
                                           routing_key=publish_request.get('routing_key'),
                                           body=publish_request.get('body'),
                                           properties=pika.BasicProperties(delivery_mode=2),
                                           mandatory=False)
            self.publisher_results.put((True, publish_request))
        except Exception as e:
            logger.error(f'Publish ERROR = {e}')
            self.publisher_results.put((False, e))

    def _start_publishing(self):
        self.publisher_queue = Queue()
        self.publisher_results = Queue()
        self.stop_publisher = threading.Event()
        self.publisher_thread = threading.Thread(target=self.run_publisher, name="SkafosQueuePublisher", args=(self.stop_publisher,))
        self.publisher_thread.daemon = True
        self.publisher_thread.start()
        # Loop to get a connection that's established - sleep for a second
        while (not self.publisher_connection):
            logger.debug(f'No Skafos Queue connection found. Sleeping.')
            sleep(1)
        logger.info(f'Found Skafos Queue connection. Publisher ready.')

    def publish(self, routing_key, body, exchange_name=None):
        """Publish records to a Skafos Queue in blocking fashion.
           Parameters
           ----------
           routing_key : str
               Key used to route messages to the proper Skafos Queue. Must already be setup.
           body : str
               Message to publish to the Skafos Queue.
           exchange_name : str
               Name of the exchange to publish to. Defaults to None and inherits the name used in the setup method.

           Returns
           -------
           status : boolean
               Whether or not the publishing operation was successful.

           Raises
           ------
           QueueError: Thrown when inputs are mismatched or for other arbitrary publishing errors.
        """
        if not isinstance(routing_key, str):
            raise QueueError('Must use a proper routing key of type string.')
        if not exchange_name:
            exchange_name = self.exchange_name
        if not self.publisher_queue:
            self._start_publishing()
        if isinstance(body, str):
            self.publisher_queue.put({'body': body, 'routing_key': routing_key, 'exchange_name': exchange_name, 'status': 'running'})
            self.publisher_connection.add_callback_threadsafe(lambda: self.__publish())
            status, msg = self.publisher_results.get(block=True)
            return status
        else:
            raise QueueError('Skafos Queue Message must be of type string.')

    def on_message(self, channel, method, properties, body):
        t = method, properties, body
        logger.debug(f'recieved {t}')
        self.consume_queue.put(t)

    def __consuming(self, queue_name):
        try:
            res = self.consumer_channel.basic_consume(self.on_message, queue=queue_name)
        except Exception as e:
            logger.error(f'Consume ERROR = {e}')
            t = None, None, e
            self.consume_queue.put(t)

    def _start_consuming(self, queue_name):
        self.consume_queue = Queue()
        self.stop_consumer = threading.Event()
        self.consumer_thread = threading.Thread(target=self.run_consumer, name="SkafosQueueConsumer", args=(self.stop_consumer,))
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        # TODO - clean this up with some retry logic
        # Loop to get a connection that's established - sleep for a second
        while (not self.consumer_connection):
            logger.debug(f'No Skafos Queue connection found. Sleeping.')
            sleep(1)
        logger.info(f'Found Skafos Queue connection. Starting to consume.')
        self.consumer_connection.add_callback_threadsafe(lambda: self.__consuming(queue_name))


    def consume(self, queue_name=None, wait_timeout=None, auto_ack=False):
        """Consume records from a Skafos Queue one-by-one, in blocking fashion.
           Parameters
           ----------
           queue_name : str
               Names of the individual queue to consume from. Must already be setup and have published to it. Defaults to None.
           wait_timeout : int or float
               Number of seconds to wait for items to come off the queue. Will throw an Empty error if timeout is exceeded. Defaults to None.
           auto_ack : boolean
               Whether or not to automatically acknowledge messages from the queue while consuming. Defaults to False.

           Returns (tuple)
           -------
           method : str
           properties : str
           body : str
               Message retrieved from the Skafos Queue

           Raises
           ------
           QueueError: Thrown when inputs are mismatched or for other arbitrary conusming errors.
           Empty: Queue is now empty!
        """
        if not isinstance(queue_name, str):
            raise QueueError('Must pass a queue name that is a valid str.')
        if not self.consume_queue:
            self._start_consuming(queue_name)
        try:
            method, properties, body = self.consume_queue.get(block=True, timeout=wait_timeout)
            if not method and not properties:
                raise body
            if auto_ack:
                self.ack(method.delivery_tag)
            return method, properties, body
        except Empty:
            raise Empty('Queue is Empty!')
        except Exception as e:
           raise QueueError('Consume ERROR = {e}')
