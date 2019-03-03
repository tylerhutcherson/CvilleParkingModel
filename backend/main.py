import json
from time import time
from queue import Empty
from datetime import datetime

from skafossdk import *

from messaging.skafos_queue import SkafosQueue
import model.schema as s
from model import features

def _evaluate_parking_model(msg, model):
    # Map input lat lon to a block id
    res = features.closest_block(msg)
    msg['block_id'] = res['block_id']


    model_inputs = features.create(msg)
    msg.update({
        'ticket_likelihood': 0.67,#model.predict_proba(model_inputs)[0],
        'created_at': datetime.now().strftime(s.TS_FORMAT)
    })
    return msg

def _publish_response(response, queue):
    res = queue.publish(
        exchange_name=s.EXCHANGE_NAME,
        routing_key=s.RESULTS_ROUTING_KEY,
        body=json.dumps(response)
    )

def consume_input_queue(skafos, queue, model):
    try:
        while True:
            # Retrieve a message from input queue
            method, properties, body = queue.consume(
              queue_name=s.INPUT_QUEUE_NAME,
              wait_timeout=300
            )
            # Start the timer
            start_time = time()
            # Load message and make a prediction
            msg = json.loads(body)
            ska.log(msg, labels=['predict', 'message'])
            response = _evaluate_parking_model(msg, model)
            # Publish response to the results queue
            _publish_response(response, queue)
            queue.ack(method.delivery_tag)
            # Deliver performance metrics back to user
            runtime = time() - start_time
            skafos.log(f'Prediction delivered in {runtime} seconds', labels=['report'])
    except Empty:
        skafos.log("Queue Empty! Packing up and going home.", labels=['predict'])
    except Exception as e:
        skafos.log(f'Error while scoring: {e}', labels=['error', 'predict'])
        raise e
    skafos.queue.stop()


if __name__ == "__main__":
    # Initialize Skafos and Queue
    ska = Skafos()
    queue = SkafosQueue()

    # Load parking model - update soon
    parking_model = None

    # Consume from input queue
    consume_input_queue(skafos=ska, queue=queue, model=parking_model)
    ska.log("Closing application. Goodbye.", labels=['shutdown'])
