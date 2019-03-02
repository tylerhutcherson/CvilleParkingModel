import json
from logger import logger
from blah import SkafosQueue

queue = SkafosQueue()
modes = ['results', 'input']
keys = [m + '-key' for m in modes]
queue.setup(exchange_name="CvilleParkingModel", queue_names=modes, routing_keys = keys, delete = True)

lat = input("Latitude: ")
lon = input("Longitude: ")

msg = {
    'lat': lat,
    'lon': lon,
    'routing_key': keys[0]
}

res = queue.publish('input-key', json.dumps(msg), "CvilleParkingModel")
logger.debug(res)

try:
    method, properties, body = queue.consume(queue_name = 'results')
    json_body = json.loads(body)
    logger.debug(json.dumps(json_body))
    queue.ack(method.delivery_tag)
except Exception as e:
    queue.ack(method.delivery_tag)
    logger.error(f'{e}')
