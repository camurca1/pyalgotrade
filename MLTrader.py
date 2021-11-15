import pickle
import zmq
import MetaTrader5 as mt5
import numpy as np
import pandas as pd
import datetime as dt
import pytz
import time


timezone = pytz.timezone('UTC')
log_file = 'log/ml_strategy_automated.log'
algorithm = pickle.load(open('models/algorithm.pkl', 'rb'))

with open(log_file, 'w') as f:
    f.write('**** NOVO ARQUIVO DE LOG ***\n')
    f.write(str(dt.datetime.now()) + '\n\n\n')


def logger_monitor(message, time=True, sep=True):
    """ Função para monitoramento e log.
    """
    with open(log_file, 'a') as f:
        t = str(dt.datetime.now())
        msg = ''
        if time:
            msg += '\n' + t + '\n'
        if sep:
            msg += 80 * '=' + '\n'
        msg += str(message) + '\n\n'
        f.write(msg)


class Mt5Handler:
    def __init__(self):
        self.symbol_info = None

        if not mt5.initialize():
            print('Falha na inicialização. ERRO: ', mt5.last_error())
            quit()
        else:
            print(mt5.version())
            print('Conectado com sucesso. \n\n')
            account_info_ = mt5.account_info()._asdict()
            account_info_ = pd.DataFrame(list(account_info_.items()), columns=['property', 'value'])
            print(account_info_, end='\n \n')

    def get_instruments(self, verbose=False):
        symbols = mt5.symbols_get()

        if verbose:
            for symbol in symbols:
                print(symbol.name + '\n')

        return sorted(symbols)

    def get_symbol_info(self, instrument):
        info = mt5.symbol_info(instrument)

        return info

    def get_prices(self, instrument):
        """ Retorna os preços de BID/ASK atuais.
        """
        selected = mt5.symbol_select(instrument, True)
        if selected:
            time = mt5.symbol_info_tick(instrument).time
            time = dt.datetime.utcfromtimestamp(time)
            bid = float(mt5.symbol_info_tick(instrument).bid)
            ask = float(mt5.symbol_info_tick(instrument).ask)
            return time, bid, ask
        else:
            raise ValueError(f'Erro ao selecionar o ativo {instrument}. Verifique se existe.')

    def transform_datetime(self, dati):
        """ Transforma datetime do Python para string.
        """
        time = str(dt.datetime.utcfromtimestamp(dati))
        return time

    def retrieve_data(self, instrument, timeframe, intraday):
        if intraday:
            csv_path = f'dados/{instrument}_intraday_mt5timeframe_{timeframe}.csv'
        else:
            csv_path = f'dados/{instrument}_daily.csv'

        data = pd.read_csv(csv_path, index_col=['datetime'], parse_dates=True).dropna()
        data = pd.DataFrame(data[['open', 'high', 'low', 'close', 'real_volume']])
        data.rename(columns={'real_volume': 'volume'}, inplace=True)
        self.symbol_info = self.get_symbol_info(symbol)
        return data

    def create_order(self, action, instrument, lot_size, sl_points, tp_points, deviation):
        lot = self.get_symbol_info(instrument).volume_min * lot_size

        if action == 1:
            trade_type = mt5.ORDER_TYPE_BUY
            price = mt5.symbol_info_tick(instrument).ask
        elif action == -1:
            trade_type = mt5.ORDER_TYPE_SELL
            price = mt5.symbol_info_tick(instrument).bid
        point = mt5.symbol_info(instrument).point

        comment = "CAMURCABOT"

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": instrument,
            "volume": lot,
            "type": trade_type,
            "price": price,
            "sl": price - sl_points * point,
            "tp": price + tp_points * point,
            "deviation": deviation,
            "magic": 234000,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN
        }

        return request

    def send_order(self, request):
        result = mt5.order_send(request)
        print(request)
        print(mt5.last_error())
        print(f"Ordem enviada. {result.retcode == mt5.TRADE_RETCODE_DONE}")
        return result

    def remote_send(self, socket, data):
        try:
            socket.send_string(data)
            msg = socket.recv_string()
            return msg
        except zmq.Again as e:
            print("Waiting for PUSH from MetaTrader 5")

    def stream_data(self, instrument):
        context = zmq.Context()
        reqSocket = context.socket(zmq.REQ)
        reqSocket.connect("tcp://localhost:5555")
        data_stream = self.remote_send(reqSocket, f"RATES|{instrument}")
        raw = data_stream.split(',')
        data = {"symbol": symbol,
                "time": trader.transform_datetime(int(raw[0])),
                "ask": float(raw[1]),
                "bid": float(raw[2]),
                "buy_volume": int(raw[3]),
                "sell_volume": int(raw[4]),
                "tick_volume": int(raw[5]),
                "real_volume": int(raw[6]),
                "buy_volume_market": int(raw[7]),
                "sell_volume_market": int(raw[8])}
        self.on_success(data['time'], data['bid'], data['ask'], False)
        return data


class MLTrader(Mt5Handler):
    def __init__(self, algorithm):
        self.model = algorithm['model']
        self.mu = algorithm['mu']
        self.std = algorithm['std']
        self.units = 100
        self.position = 0
        self.bar = '2s'
        self.window = 2
        self.lags = 3
        self.min_length = self.lags + self.window + 1
        self.features = ['return', 'vol', 'mom', 'sma', 'min', 'max']
        self.raw_data = pd.DataFrame()

    def prepare_features(self):
        self.data['return'] = np.log(self.data['mid'] / self.data['mid'].shift(1))
        self.data['vol'] = self.data['return'].rolling(self.window).std()
        self.data['mom'] = np.sign(self.data['return'].rolling(self.window).mean())
        self.data['sma'] = self.data['mid'].rolling(self.window).mean()
        self.data['min'] = self.data['mid'].rolling(self.window).min()
        self.data['max'] = self.data['mid'].rolling(self.window).max()
        self.data.dropna(inplace=True)
        self.data[self.features] -= self.mu
        self.data[self.features] /= self.std
        self.cols = []

        for f in self.features:
            for lag in range(1, self.lags + 1):
                col = f'{f}_lag_{lag}'
                self.data[col] = self.data[f].shift(lag)
                self.cols.append(col)

    def report_trade(self, pos, order):
        """ Imprime, registra e envia dados do trade.
        """
        out = '\n\n' + 80 * '=' + '\n'
        out += '*** POSIÇÃO {} *** \n'.format(pos) + '\n'
        out += str(order) + '\n'
        out += 80 * '=' + '\n'
        logger_monitor(out)
        print(out)

    def on_success(self, time, bid, ask, backtest=False):
        df = pd.DataFrame({"bid": float(bid),
                           "ask": float(ask)},
                          index=[pd.Timestamp(time).tz_localize(None)])
        self.raw_data = self.raw_data.append(df)
        self.data = self.raw_data.resample(self.bar, label='right').last().ffill()
        self.data = self.data.iloc[:-1]
        if len(self.data) > self.min_length:
            self.min_length += 1
            self.data['mid'] = (self.data['bid'] + self.data['ask']) / 2
            self.prepare_features()
            features = self.data[self.cols].iloc[-1].values.reshape(1, -1)
            signal = self.model.predict(features)[0]
            logger_monitor('MOST RECENT DATA\n' +
                           str(self.data[self.cols].tail()),
                           False)

            logger_monitor('features:\n' + str(features) + '\n' +
                           'position: ' + str(self.position) + '\n' +
                           'signal:   ' + str(signal), False)

            if self.position in [0, -1] and signal == 1:
                # order_size = self.get_symbol_info.volume_min * 100
                if not backtest:
                    # order = self.create_order(signal, symbol, order_size, 10, 20, 20)
                    order = f"COMPREI EM {ask}"
                else:
                    order = self.remote_send(self.reqSocket, f'TRADE|OPEN')
                self.report_trade('LONG', order)
                self.position = 1
            elif self.position in [0, 1] and signal == -1:
                if not backtest:
                    # order = self.create_order(signal, symbol, order_size, 10, 20, 20)
                    order = f"VENDI EM {bid}"
                else:
                    order = self.remote_send(self.reqSocket, f'TRADE|CLOSE')
                self.report_trade('SHORT', order)
                self.position = -1
            else:
                logger_monitor('*** NO TRADE PLACED ***')

            logger_monitor('*** END OD CYCLE ***\n\n', False, False)


symbol = "ITUB4"

trader = MLTrader(algorithm)

while True:
    data = trader.stream_data(symbol)
    print(data)
    logger_monitor(data)
    # order = trader.create_order(1, symbol, 1, 0, 0, 20)
    time.sleep(2)

