import pandas as pd
import numpy as np
import pickle
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import AdaBoostClassifier

intraday = True
timeframe = 5
symbol = 'ITUB4'
window = 2
lags = 3

if intraday:
    csv_path = f'dados/{symbol}_intraday_mt5timeframe_{timeframe}.csv'
else:
    csv_path = f'dados/{symbol}_daily.csv'

raw = pd.read_csv(csv_path, index_col=['datetime'], parse_dates=True).dropna()
raw = pd.DataFrame(raw[['open', 'high', 'low', 'close', 'real_volume']])
raw.rename(columns={'real_volume': 'volume'}, inplace=True)

data = pd.DataFrame(raw['close'])
data['return'] = np.log(data / data.shift(1))
data['vol'] = data['return'].rolling(window).std()
data['mom'] = np.sign(data['return'].rolling(window).mean())
data['sma'] = data['close'].rolling(window).mean()
data['min'] = data['close'].rolling(window).min()
data['max'] = data['close'].rolling(window).max()
data.dropna(inplace=True)

features = ['return', 'vol', 'mom', 'sma', 'min', 'max']

cols = []
for f in features:
    for lag in range(1, lags + 1):
        col = f'{f}_lag_{lag}'
        data[col] = data[f].shift(lag)
        cols.append(col)

data.dropna(inplace=True)
data['direction'] = np.where(data['return'] > 0, 1, -1)

n_estimators = 15
random_state = 100
max_depth = 2
min_samples_leaf = 15
subsample = 0.33

dtc = DecisionTreeClassifier(random_state=random_state,
                             max_depth=max_depth,
                             min_samples_leaf=min_samples_leaf)

model = AdaBoostClassifier(base_estimator=dtc,
                           n_estimators=n_estimators,
                           random_state=random_state)

split = int(len(data) * 0.7)
train = data.iloc[:split].copy()
mu, std = train.mean(), train.std()
train_ = (train - mu) / std

model.fit(train_[cols].values, train['direction'].values)

print(accuracy_score(train['direction'], model.predict(train_[cols])))

test = data.iloc[split:].copy()
test_ = (test - mu) / std
test['position'] = model.predict(test_[cols])

print(accuracy_score(test['direction'], test['position']))

test['strategy'] = test['position'] * test['return']
test[['return', 'strategy']].cumsum().apply(np.exp).plot(figsize=(10, 6))
algorithm = {'model': model, 'mu': mu, 'std': std}
pickle.dump(algorithm, open('models/algorithm.pkl', 'wb'))
