# %%

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from talib import RSI, BBANDS, MACD

# %%
# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('use_inf_as_na', True)
sns.set_style('whitegrid')
idx = pd.IndexSlice

# %%
DATA_STORE = 'dados/assets.h5'

# %%
with pd.HDFStore(DATA_STORE) as store:
    data = (store['prices']
            .loc[idx['2010':'2022', 'WEGE3'], ['open', 'high', 'low', 'close', 'real_volume']]
            .reset_index('TckrSymb', drop=True))

# %%
data.info()

# %%
# Calc Bollinger Bands
up, mid, low = BBANDS(data.close, timeperiod=21, nbdevup=2, nbdevdn=2, matype=0)

# %%
# Calc RSI
rsi = RSI(data.close, timeperiod=14)

# %%
macd, macdsignal, macdhist = MACD(data.close, fastperiod=12, slowperiod=26, signalperiod=9)

# %%
macd_data = pd.DataFrame({'AAPL': data.close, 'MACD': macd, 'MACD Signal': macdsignal, 'MACD History': macdhist})

fig, axes= plt.subplots(nrows=2, figsize=(15, 8))
macd_data.AAPL.plot(ax=axes[0])
macd_data.drop('AAPL', axis=1).plot(ax=axes[1])
fig.tight_layout()
sns.despine();
plt.savefig('figs/macd_wege3')

# %%
data = pd.DataFrame({'AAPL': data.close, 'BB Up': up, 'BB Mid': mid, 'BB down': low, 'RSI': rsi, 'MACD': macd})

# %%
plt.clf()
fig, axes= plt.subplots(nrows=3, figsize=(15, 10), sharex=True)
data.drop(['RSI', 'MACD'], axis=1).plot(ax=axes[0], lw=1, title='Bollinger Bands')
data['RSI'].plot(ax=axes[1], lw=1, title='Relative Strength Index')
axes[1].axhline(70, lw=1, ls='--', c='k')
axes[1].axhline(30, lw=1, ls='--', c='k')
data.MACD.plot(ax=axes[2], lw=1, title='Moving Average Convergence/Divergence', rot=0)
axes[2].set_xlabel('')
fig.tight_layout()
sns.despine();
plt.savefig('figs/bband_rsi_wege3')