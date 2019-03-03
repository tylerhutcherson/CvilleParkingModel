from math import cos, asin, sqrt

import pandas as pd
from datetime import datetime

# Load data to map an input lat lon
inputs = ['block', 'hour', 'month', 'dayofweek']
block_columns = ['lat', 'lon', 'street', 'block_id']
block_lat_lons = pd.read_csv("s3://skafos.parking.tickets/blocklatlong.csv")
block_lat_lons.columns = block_columns
block_lat_lons = block_lat_lons[['lat', 'lon', 'block_id']].to_dict('records')

def create(msg):
   now = datetime.now()
   return pd.DataFrame(data=[{
      'block': str(msg['block_id']),
      'hour': str(now.hour),
      'month': str(now.month),
      'dayofweek': str(now.weekday())
   }])[inputs]


def _distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a))

def closest_block(msg):
    return min(block_lat_lons, key=lambda p: _distance(msg['lat'],msg['lon'],p['lat'],p['lon']))
