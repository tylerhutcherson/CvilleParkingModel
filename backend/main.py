import json
import pickle
from time import time
from queue import Empty
from datetime import datetime

from skafossdk import *

import model.schema as s
from model import features

def _evaluate_parking_model(msg, model, labeler):
    # Map input lat lon to a block id
    res = features.closest_block(msg)
    msg['block_id'] = res['block_id']

    model_inputs = features.create(msg, labeler)
    msg.update({
        'ticket_likelihood': model.predict_proba(model_inputs)[0][1],
        'created_at': datetime.now().strftime(s.TS_FORMAT)
    })
    return msg

def _publish_response(ska, response):
    res = ska.queue.publish(
        exchange_name=s.EXCHANGE_NAME,
        routing_key=s.RESULTS_ROUTING_KEY,
        body=json.dumps(response)
    )

def consume_input_queue(skafos, model, labeler):
    try:
        while True:
            # Retrieve a message from input queue
            method, properties, body = skafos.queue.consume(
              queue_name=s.INPUT_QUEUE_NAME,
              wait_timeout=46800
            )
            # Start the timer
            start_time = time()
            # Load message and make a prediction
            msg = json.loads(body)
            skafos.log(msg, labels=['predict', 'message'])
            response = _evaluate_parking_model(msg, model, labeler)
            # Publish response to the results queue
            _publish_response(ska=skafos, response=response)
            skafos.queue.ack(method.delivery_tag)
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

    # Load parking model - update soon
    loaded_model_data = ska.engine.load_model('parking_model', tag = 'latest').result()
    parking_model = pickle.loads(loaded_model_data['data'])

    # Load label encoder used
    label_encoder_data = ska.engine.load_model('label_encoder', tag = 'latest').result()
    label_encoder = pickle.loads(label_encoder_data['data'])


    # Consume from input queue
    consume_input_queue(skafos=ska,model=parking_model, labeler=label_encoder)
    ska.log("Closing application. Goodbye.", labels=['shutdown'])
