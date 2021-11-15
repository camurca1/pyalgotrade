import MetaTrader5 as mt5
import pandas as pd
import pytz
import os.path
from datetime import datetime

# configurar exibição pandas
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)

# configurar fuso horário
timezone = pytz.timezone('UTC')


def get_instruments_info():
    symbols = mt5.symbols_get()
    data_info = pd.DataFrame()
    count = 1
    total = len(symbols)
    for symbol in symbols:
        raw_info = mt5.symbol_info(symbol.name)._asdict()
        raw_info = pd.DataFrame([raw_info])
        data_info = data_info.append(raw_info)
        print(f'(Ativo {count} de {total}) Os dados de {symbol.name} foram registrados.')
        count += 1

    return data_info


def get_ohlcv(simbolo, timeframe, intraday=False):
    if intraday:
        csv_path = f'dados/cotacoes/{simbolo}_intraday_mt5timeframe_{timeframe}.csv'
        hdf5_path = f'dados/cotacoes/{simbolo}_intraday_mt5timeframe_{timeframe}.h5'
    else:
        csv_path = f'dados/cotacoes/{simbolo}_daily.csv'
        hdf5_path = f'dados/cotacoes/{simbolo}_daily.h5'

    if os.path.isfile(csv_path):
        try:
            ativo_precos = pd.read_csv(csv_path, parse_dates=['datetime'])
            ultima_data = ativo_precos['datetime'].iloc[-1]
            ultima_data = datetime(ultima_data.year, ultima_data.month, ultima_data.day,
                                   ultima_data.hour, ultima_data.minute + 1, tzinfo=timezone)
            agora = datetime(datetime.now().year, datetime.now().month, datetime.now().day,
                             datetime.now().hour, datetime.now().minute, tzinfo=timezone)
            novos_precos = pd.DataFrame(mt5.copy_rates_range(simbolo, timeframe, ultima_data, agora))
            novos_precos['datetime'] = pd.to_datetime(novos_precos['time'], unit='s')
            novos_precos = novos_precos[['datetime', 'time', 'open', 'high', 'low',
                                         'close', 'tick_volume', 'spread', 'real_volume']]
            novos_precos.to_csv(csv_path, mode='a', header=False, index=False)
            novos_precos.to_hdf(hdf5_path, 'data', format='table')
            print(f'Cotações de {simbolo} atualizadas em:')
            print(f'{csv_path}\n{hdf5_path}\n\n')
        except:
            pass
    else:
        try:
            ativo_precos = pd.DataFrame(mt5.copy_rates_from_pos(simbolo, timeframe, 0, 99999))
            ativo_precos['datetime'] = pd.to_datetime(ativo_precos['time'], unit='s')
            ativo_precos = ativo_precos[['datetime', 'time', 'open', 'high', 'low',
                                         'close', 'tick_volume', 'spread', 'real_volume']]
            ativo_precos.to_csv(csv_path, index=False)
            with pd.HDFStore(hdf5_path) as store:
                store.put('file', ativo_precos)
            print(f'Cotações de {simbolo} salvas em:')
            print(f'{csv_path}\n{hdf5_path}\n\n')
        except:
            pass


def atualizar_ativos(atualizar=False):
    # recuperar todos os ativos disponiveis
    if atualizar:
        ativos_completos = get_instruments_info()
        ativos_completos.to_csv('dados/todos_ativos.csv', index=False)
        acoes_brasileiras = ativos_completos[ativos_completos['isin'].str.contains('ACN')]
        acoes_brasileiras.to_csv('dados/acoes_brasileiras.csv', index=False)


def atualizar_cotacoes(timeframes, ativos, atualizar=False):
    # recuperar dados históricos
    if atualizar:
        for timeframe in timeframes:
            for ativo in ativos:
                if timeframe != 16408:
                    get_ohlcv(ativo, timeframe, True)
                else:
                    get_ohlcv(ativo, timeframe)


def empilhar_dados(ativos):
    precos = pd.DataFrame()

    for ativo in ativos:
        try:
            preco = pd.read_csv(f'dados/cotacoes/{ativo}_daily.csv',
                                parse_dates=True,
                                infer_datetime_format=True)
            preco['TckrSymb'] = ativo
            precos = precos.append(preco,
                                   ignore_index=False,
                                   verify_integrity=False)
        except:
            print(f'{ativo} não encontrado.')
    precos['datetime'] = pd.to_datetime(precos['datetime'])
    precos.rename(columns={'datetime': 'DT_REFER'}, inplace=True)
    precos.set_index(['DT_REFER', 'TckrSymb'], inplace=True)
    precos.sort_index(inplace=True)
    return precos


# configurar exibição pandas
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)

# iniciar o MT5
if not mt5.initialize():
    print('Falha na inicialização. ERRO: ', mt5.last_error())
    quit()
else:
    print(mt5.version())
    account_info_ = mt5.account_info()._asdict()
    account_info_ = pd.DataFrame(list(account_info_.items()), columns=['property', 'value'])
    print(account_info_, end='\n \n')

# atualizar infos
atualizar_ativos(False)

# carregar arquivo de equities
ativos_equities = pd.read_csv(f'dados/empresas_monitoradas.csv', low_memory=False)

# definir ativos e timeframes
ativos = ativos_equities['TckrSymb'].tolist()
timeframes = [mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15,
              mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_D1]

atualizar_cotacoes(timeframes, ativos, False)

precos_ativos = empilhar_dados(ativos)
DATA_STORE = 'dados/assets.h5'

with pd.HDFStore(DATA_STORE) as store:
    store.put('prices', precos_ativos)
    print(store.info())

mt5.shutdown()
