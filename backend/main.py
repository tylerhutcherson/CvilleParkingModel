import os
import json
from time import time
from queue import Empty
from datetime import datetime

from skafossdk import *
from skafossdk.exceptions import QueueError

import model.schema as s
from model import features

def setup_queues(skafos):
  skafos.log('Setting up application queues.', labels=['queue_setup'])
  skafos.queue.setup(
    exchange_name=s.EXCHANGE_NAME,
    queue_names=[s.INPUT_QUEUE_NAME, s.RESULTS_QUEUE_NAME],
    routing_keys=[s.INPUT_ROUTING_KEY, s.RESULTS_ROUTING_KEY],
    delete=True
  )


def _evaluate_parking_model(msg, model):
    model_inputs = features.create(msg)
    msg.update({
        'ticket_likelihood': model.predict_proba(model_inputs)[0],
        'created_at': datetime.now().strftime(s.TS_FORMAT)
    })
    return msg

def _publish_response(response):
    res = ska.queue.publish(
        exchange_name=s.EXCHANGE_NAME,
        routing_key=s.RESULTS_ROUTING_KEY,
        body=json.dumps(response)
    )

def consume_input_queue(skafos, model):
  try:
    while True:
      # Retrieve a message from input queue
      method, properties, body = skafos.queue.consume(
        queue_name=s.INPUT_QUEUE_NAME
      )
      # Start the timer
      start_time = time()
      # Load message and make a prediction
      msg = json.loads(body)
      response = _evaluate_parking_model(msg, model)
      # Publish response to the results queue
      _publish_response(response)
      skafos.queue.ack(method.delivery_tag)
      # Deliver performance metrics back to user
      runtime = time() - start_time
      skafos.log(f'Prediction delivered in {runtime} seconds', labels=['report'])
  except Empty:
    skafos.log("Queue Empty! Packing up and going home.", labels=['predict'])
  except QueueError as e:
    skafos.log(f'Error w/ queue while generating predictions: {e}', labels=['error', 'predict'])
    raise e
  except Exception as e:
    skafos.log(f'Error while scoring: {e}', labels=['error', 'predict'])
    raise e
  skafos.queue.stop()


if __name__ == "__main__":
  # Initialize Skafos
  ska = Skafos()

  # Setup queues
  setup_queues(skafos=ska)

  # Load parking model

  # Consume from input queue
  consume_input_queue(skafos=ska, model=parking_model)
  ska.log("Closing application. Goodbye.", labels=['shutdown'])
