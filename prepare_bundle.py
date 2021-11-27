# %%
import pandas as pd
import exchange_calendars as xcals

bmf = xcals.get_calendar('BVMF')
start, end = '2010-01-01', '2021-11-25'

data_dir = 'dados/cotacoes/'
ativos = pd.read_csv(f'dados/empresas_monitoradas.csv', low_memory=False)
ativos = ativos['TckrSymb'].tolist()
timeframes = ['daily', '1', '5', '15', '30', '16385']


# %%
def save_csv_bundle(ativo, timeframe=None, intraday=False):
    if intraday:
        df = pd.read_csv(f'{data_dir}{ativo}_daily.csv', parse_dates=True)
        csv_dir = 'dados/cotacoes/bundle_csv_dir/daily/'
    else:
        df = pd.read_csv(f'{data_dir}{ativo}_intraday_mt5timeframe_{timeframe}.csv', parse_dates=True)
        if timeframe == '1':
            csv_dir = 'dados/cotacoes/bundle_csv_dir/m1/'
        elif timeframe == '5':
            csv_dir = 'dados/cotacoes/bundle_csv_dir/m5/'
        elif timeframe == '15':
            csv_dir = 'dados/cotacoes/bundle_csv_dir/m15/'
        elif timeframe == '30':
            csv_dir = 'dados/cotacoes/bundle_csv_dir/m30/'
        elif timeframe == '16385':
            csv_dir = 'dados/cotacoes/bundle_csv_dir/h1/'
        else:
            return print(f'{ativo}: Timeframe {timeframe} inválido!')

    df = df[['datetime', 'open', 'high', 'low', 'close', 'real_volume']]
    df.rename({'datetime': 'date', 'real_volume': 'volume'}, axis=1, inplace=True)
    df['dividend'] = 0.0
    df['split'] = 0.0
    df.to_csv(f'{csv_dir}{ativo}.csv', index=False)
    print(f'{ativo} salvo em {csv_dir}.')

def match_index():
    pass
# %%

for ativo in ativos:
    for timeframe in timeframes:
        if timeframe == 'daily':
            try:
                save_csv_bundle(ativo, intraday=True)
            except:
                print(f'Não há cotações salvas para o ativo {ativo}, timeframe {timeframe}.')
        else:
            try:
                save_csv_bundle(ativo, timeframe)
            except:
                print(f'Não há cotações salvas para o ativo {ativo}, timeframe {timeframe}.')

#%%
df = pd.read_csv('dados/cotacoes/bundle_csv_dir/daily/AALR3F.csv', parse_dates=True, index_col=['date'])
df.index = df.index.tz_localize('UTC')
df.index

#%%

df_first_day = df.index[0]
df_last_day = df.index[-1]

#%%
schedule = bmf.schedule.loc[start:end]
schedule.index


#%%
df_sessions = schedule[schedule.index.slice_indexer(df_first_day, df_last_day)]
df_sessions = df_sessions.index.to_frame('date')
df_sessions.info()

#%%
df = df_sessions.join(df, how='left').fillna(method='ffill')
df.drop(0, axis=1, inplace=True)

#%%
df.info()