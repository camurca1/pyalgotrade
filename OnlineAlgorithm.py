import zmq
import datetime
import numpy as np
import pandas as pd

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect('tcp://0.0.0.0:5555')
socket.setsockopt_string(zmq.SUBSCRIBE, 'SYMBOL')

df = pd.DataFrame()
mom = 3
min_lenght = mom + 1

while True:
  data = socket.recv_string()
  t = datetime.datetime.now()
  sym, value = data.split()
  df = df.append(pd.DataFrame({sym: float(value)}, index=[t]))
  dr = df.resample('5s', label='right').last()
  dr['returns'] = np.log(dr / dr.shift(1))
  if len(dr) > min_lenght:
    min_lenght += 1
    dr['momentum'] = np.sign(dr['returns'].rolling(mom).mean())
    print('\n' + '=' * 51)
    print('NEW SIGNAL | {}'.format(datetime.datetime.now()))
    print('=' * 51)
    print(dr.iloc[:-1].tail())
    if dr['momentum'].iloc[-2] == 1.0:
      print('\nLong market position.')
    elif dr['momentum'].iloc[-2] == -1.0:
      print('\nShort market position.')
