from skafossdk import *
import model.schema as s

ska = Skafos()
ska.log("setting up queues", labels=['setup'])

ska.queue.setup(
    queue_names=[s.INPUT_QUEUE_NAME, s.RESULTS_QUEUE_NAME],
    routing_keys=[s.INPUT_ROUTING_KEY, s.RESULTS_ROUTING_KEY],
    delete=False,
    exchange_name=s.EXCHANGE_NAME
)
ska.log("done")
