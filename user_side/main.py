import json
from flask import Flask, abort, request, make_response
from flask_api import status
from logger import logger
from messaging.skafos_queue import SkafosQueue

api = Flask('api')
queue = SkafosQueue()
modes = ['results', 'input']
keys = [m + '-key' for m in modes]
#queue.setup(exchange_name="CvilleParkingModel", queue_names=modes, routing_keys = keys, delete = True)

@api.route("/status")
def status_endpoint():
    return json.dumps({'status':"It works Morgan!"})

@api.route("/", methods=["POST"])
def submit_endpoint():
    if not request.json or not 'lat' in request.json or not 'lon' in request.json:
        abort(400)
    msg = {
        'lat': request.json['lat'],
        'lon': request.json['lon'],
        'routing_key': keys[0]
    }

    res = queue.publish('input-key', json.dumps(msg), "CvilleParkingModel")
    if res:
        try:
            method, properties, body = queue.consume(queue_name = 'results',
                    wait_timeout=7200, auto_ack=True)
            json_body = json.loads(body)
            logger.info(json.dumps(json_body))
            res = make_response(json.dumps(json_body))
            return res, status.HTTP_200_OK
        except Exception as e:
            #queue.ack(method.delivery_tag)
            logger.error(f'{e}')        
            return json.dumps({'error': f'{e}'}), status.HTTP_400_BAD_REQUEST
    else:
        return json.dumps({'error':f'{res}'}), status.HTTP_400_BAD_REQUEST

api.run(host='0.0.0.0', port=5000, debug=True)

