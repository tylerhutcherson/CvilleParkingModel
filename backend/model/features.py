import pandas as pd
from datetime import datetime

inputs = ['block', 'hour', 'month', 'dayofweek']

def create(msg):
   now = datetime.now()
   return pd.DataFrame(data=[{
      'block': str(msg['block_id']),
      'hour': str(now.hour),
      'month': str(now.month),
      'dayofweek': str(now.weekday())
   }])[inputs]
