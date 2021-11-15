# %%
import pandas as pd
import pandas_datareader.data as web
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
import requests

import matplotlib.pyplot as plt
import seaborn as sns

# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('use_inf_as_na', True)
sns.set_style('whitegrid')
idx = pd.IndexSlice

# %%
DATA_STORE = 'dados/assets.h5'
csv_path = f'dados/mkt_cap.csv'
acoes = pd.read_csv(csv_path, parse_dates=True)
acoes['DT_REFER'] = pd.to_datetime(acoes['DT_REFER'])

# %%
acoes_df = acoes.pivot(index='DT_REFER', columns='TckrSymb', values='close').fillna(method='ffill').fillna(0)
mkt_cap = acoes.pivot(index='DT_REFER', columns='TckrSymb', values='valormercado').fillna(method='ffill').fillna(0)

# %%
monthly_prices = acoes_df.resample('M').last()

# %%
# return lags calc
outlier_cutoff = 0.01
data = pd.DataFrame()
lags = [1, 2, 3, 6, 9, 12]

for lag in lags:
    data[f'return_{lag}m'] = (monthly_prices
                              .pct_change(lag)
                              .stack()
                              .pipe(lambda x: x.clip(lower=x.quantile(outlier_cutoff),
                                                     upper=x.quantile(1 - outlier_cutoff)))
                              .add(1)
                              .pow(1 / lag)
                              .sub(1))
data = data.swaplevel().fillna(0)

# %%
min_obs = 108
nobs = data.groupby(level='TckrSymb').size()
keep = nobs[nobs >= min_obs].index

data = data.loc[idx[keep, :], :]

# %%
factors = ['RmRf', 'SMB', 'HML', 'WML', 'IML']
factor_data = pd.read_csv('dados/fama_factors.csv', parse_dates=True, index_col='DT_REFER')

# %%
factor_data = factor_data.join(data['return_1m']).sort_index()

# %%
T = 24
betas = (factor_data.groupby(level='TckrSymb',
                             group_keys=False)
         .apply(lambda x: RollingOLS(endog=x.return_1m,
                                     exog=sm.add_constant(x.drop('return_1m', axis=1)),
                                     window=min(T, x.shape[0] - 1))
                .fit(params_only=True)
                .params
                .drop('const', axis=1)))

# %%
data = (data
        .join(betas
              .groupby(level='TckrSymb')
              .shift()))
data.loc[:, factors] = data.groupby('TckrSymb')[factors].apply(lambda x: x.fillna(x.mean()))

# %%
for lag in [2, 3, 6, 9, 12]:
    data[f'momentum_{lag}'] = data[f'return_{lag}m'].sub(data.return_1m)
data[f'momentum_3_12'] = data[f'return_12m'].sub(data.return_3m)

# %%
dates = data.index.get_level_values('DT_REFER')
data['year'] = dates.year
data['month'] = dates.month

# %%
for t in range(1, 7):
    data[f'return_1m_t-{t}'] = data.groupby(level='TckrSymb').return_1m.shift(t)
# %%
for t in [1, 2, 3, 6, 12]:
    data[f'target_{t}m'] = data.groupby(level='TckrSymb')[f'return_{t}m'].shift(-t)

# %%
stocks = pd.read_csv('dados/empresas_monitoradas.csv')
stocks = stocks[stocks['SgmtNm'] != 'ODD LOT']
stocks = stocks[['TckrSymb', 'SETOR_ATIV', 'DT_REG']]
stocks['DT_REG'] = pd.to_datetime(stocks['DT_REG'], dayfirst=True)
stocks['DT_REG'] = stocks['DT_REG'].dt.year
stocks.drop_duplicates(inplace=True)
stocks.set_index('TckrSymb', inplace=True)
acoes = acoes.join(stocks, on='TckrSymb', how='left')
# %%
quintiles = (pd.qcut(stocks.DT_REG, q=5, labels=list(range(1, 6)))
             .astype(float)
             .fillna(0)
             .astype(int)
             .to_frame('age'))
# %%
data = data.join(quintiles, on='TckrSymb', how='left')
data.age = data.age.fillna(-1)

# %%
size_factor = (monthly_prices
               .loc[data.index.get_level_values('DT_REFER').unique(),
                    data.index.get_level_values('TckrSymb').unique()]
               .sort_index()
               .pct_change()
               .fillna(0)
               .add(1)
               .cumprod())

# %%
mkt_cap = mkt_cap.loc[:, size_factor.columns]
mkt_cap = mkt_cap.resample('M').last().sort_index()
mkt_cap.drop(['2011-12-31'], inplace=True)

# %%
msize = size_factor * mkt_cap
# %%
data['msize'] = (msize
                 .apply(lambda x: pd.qcut(x, q=10, labels=list(range(1, 11)))
                        .astype(int), axis=1)
                 .stack()
                 .swaplevel())
data.msize = data.msize.fillna(-1)
# %%
data = data.join(stocks[['SETOR_ATIV']], on='TckrSymb', how='left')

# %%
dummy_data = pd.get_dummies(data,
                            columns=['year', 'month', 'msize', 'age', 'SETOR_ATIV'],
                            prefix=['year', 'month', 'msize', 'age', ''],
                            prefix_sep=['_', '_', '_', '_', ''])
dummy_data = dummy_data.rename(columns={c: c.replace('.0', '') for c in dummy_data.columns})

# %%
ibov = web.DataReader('^BVSP', 'yahoo', '01-01-2011', '15-11-2020')
ibov.rename(columns=str.lower, inplace=True)

# %%
url = 'https://br.advfn.com/indice/ibovespa'
header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"}
r = requests.get(url, header)
ibov_teorico = pd.read_html(r.text)[3]

# %%
ibov_teorico = ibov_teorico['Ativo'].to_frame('TckrSymb')

# %%
with pd.HDFStore(DATA_STORE) as store:
    store.put('stocks', acoes)
    store.put('ibov', ibov)
    store.put('ibov_teorico', ibov_teorico)
    store.put('engineered_features', data)
    store.put('engineered_features_dummy', dummy_data)
    print(store.info())
