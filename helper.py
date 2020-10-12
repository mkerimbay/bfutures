import pandas as pd
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 500)
import numpy as np
import sys
import traceback
import json
from _myconfig import *
from deepdiff import DeepDiff

# Align directory
import os
abspath = os.path.abspath(__file__)
curr_path = os.path.dirname(abspath)
os.chdir(curr_path)


# LOGGER
import logging
from logging import config
logging.config.fileConfig('./_logger.conf')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# Binance client
from binance.client import Client
client = Client(bin_key, bin_secret)

# SQlite3
import sqlite3 as sl
con = sl.connect(fname_db)
cur = con.cursor()


def hlv_value(s):
  if s['close'] > s['smaHigh']:
    return 1
  elif s['close'] < s['smaLow']:
    return -1
  else:
    return np.nan


def ssl_green(s):
  if s['hlv'] < 0:
    return s['smaLow']
  return s['smaHigh']


def ssl_red(s):
  if s['hlv'] < 0:
    return s['smaHigh']
  return s['smaLow']


def table_exists(s):
    q = "select count(*) from sqlite_master where type='table' and name=?"
    params = (s,)
    cur.execute(q, params)
    return cur.fetchone()[0]


def last_time(s):
    q = 'select * from ' + s + ' order by rowid desc limit 1'
    cur.execute(q)
    df = pd.read_sql(q, con)
    df.time = pd.to_datetime(df.time)
    return df['time'].values[0]


def get_data_sql(s):
    # try:
    q = 'select * from {}'.format(s)
    return pd.read_sql(q, con)
    # except Exception as e:
    #     logger.exception(e)
    #     return pd.DataFrame()


def read_json(fn):
    try:
        return json.load(open(fn))
    except Exception as e:
        logger.exception(e)
        return {}


def update_dict(dm, ds, changes=False):
    try:
        if changes:
            diff = DeepDiff(dm, ds)
            print(diff)
            if 'values_changed' in diff:
                for k,v in diff['values_changed'].items():
                    msg = str(k).replace('root', '') + ' ' + str(v)
                    logger.info(msg)
        for k in ds:
            if k not in dm:
                dm[k] = {}
            dm[k].update(ds[k])
        return dm
    except Exception as e:
        logger.exception(e)


def log_traceback(ex, ex_traceback=None):
    if ex_traceback is None:
        ex_traceback = ex.__traceback__
    tb_lines = [ line.rstrip('\n') for line in traceback.format_exception(ex.__class__, ex, ex_traceback)]
    logger.error(tb_lines)


def get_ohlcv(s,interval):
    # kl = client.futures_klines(symbol=s, interval=interval)
    try:
        kl = client.get_klines(symbol=s, interval=interval)
        df = pd.DataFrame(kl)
        df = df[[0, 1, 2, 3, 4, 5]]
        df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
        df.time = pd.to_datetime(df.time, unit='ms')
        df['open'] = df['open'].astype('float')
        df['high'] = df['high'].astype('float')
        df['low'] = df['low'].astype('float')
        df['close'] = df['close'].astype('float')
        df['volume'] = df['volume'].astype('float')
        return df
    except Exception as e:
        logger.exception(e)
        print('cant handle {}'.format(s))
        return pd.DataFrame()


def f_positions():
    try:
        df = pd.DataFrame(client.futures_position_information())
        df = df[['symbol', 'entryPrice', 'leverage', 'liquidationPrice', 'markPrice', 'positionAmt', 'unRealizedProfit']]
        df['entryPrice'] = df['entryPrice'].astype(float)
        df['liquidationPrice'] = df['liquidationPrice'].astype(float)
        df['markPrice'] = df['markPrice'].astype(float)
        df['positionAmt'] = df['positionAmt'].astype(float)
        df['unRealizedProfit'] = df['unRealizedProfit'].astype(float)
        df['leverage'] = df['leverage'].astype(int)
        return df
    except Exception as e:
        logger.exception(e)
        print('wrong')


def get_denom(d, col):
    pos = d[col].find('.')
    if pos == -1:
        return 0
    return len(d[col])-pos-1


def update_denom():
    try:
        d_old = read_json(fname_denom)
        df = pd.DataFrame(client.futures_ticker())
        df = df[['symbol', 'lastPrice', 'lastQty']]
        df = df.set_index('symbol')
        df['dPrice'] = df.apply(get_denom, args=('lastPrice',), axis=1)
        df['dQty'] = df.apply(get_denom, args=('lastQty',), axis=1)
        df = df[['dPrice', 'dQty']]
        d_new = update_dict(d_old, df.T.to_dict(), changes=True)
        json.dump(d_new, open(fname_denom, 'w+'))
        logger.info(' denoms updated')
    except Exception as e:
        logger.exception(e)


def latest_prices():
    try:
        df = pd.DataFrame(client.futures_ticker())
        df = df[['symbol', 'lastPrice']]

        return df
        # df = df.set_index('symbol')
        # d_new = df.T.to_dict()
        #
        # # notify if new tickers added
        # k1 = set(d_new.keys()) ^ set(d_old.keys())
        # for k in k1:
        #     msg = 'new ticker added - {}'.format(k)
        #     logger.info(msg)
        #     # send to telegram
        #
        # d_new = update_dict(d_old, d_new)
        # json.dump(d_new, open(fn, 'w+'))
        #
        # s = '{} file updated'.format(fn)
        # logger.info(s)
    except Exception as e:
        logger.exception(e)


def store_klines():
    symbols = f_positions()['symbol'].tolist()
    for s in symbols:
        df = get_ohlcv(s, '5m')
        if df.shape[0] > 0:
            df.drop(df.tail(1).index, inplace=True)  # not complete data yet, discard it
            if table_exists(s):
                dt_last = last_time(s)
                df = df[df['time'] > dt_last]
                df.to_sql(s, con, index=None, if_exists='append')
                msg = '* {} -> ({}) klines added'.format(s, df.shape[0])
            else:
                df.to_sql(s, con, index=None)
                msg = '* {} table created in db'.format(s)
            logger.info(msg)


def update_trend(ssl=20):
    symbols = f_positions()['symbol'].tolist()
    d_trend = {}
    for i,s in enumerate(symbols):
        d = {}
        try:
            df = get_ohlcv(s, '1d')
            # df = get_data_sql(s)
            if df.shape[0] > 0:
                df['smaHigh'] = df['high'].rolling(window=ssl).mean()
                df['smaLow'] = df['low'].rolling(window=ssl).mean()
                df['hlv'] = df.apply(hlv_value, axis=1)
                df.dropna(axis=0, inplace=True)
                if df.shape[0] > 0:
                    last_hlv = int(df.tail(1)['hlv'].values[0])
                    d['hlv'] = last_hlv
                    if df.shape[0] > 1 and len(set(df.tail(2)['hlv'].values)) > 1:
                        d['req'] = last_hlv
                        d['reqPrice'] = df.tail(1)['close'].values[0]
            if d:
                d_trend[s] = d
                print(s, d)
                print('-----')
        except Exception as e:
            logger.exception(e)
            continue

    d_master = read_json(fname_master)
    d_master = update_dict(d_master, d_trend, changes=True)
    json.dump(d_master, open(fname_master, 'w+'))
    return d_master




        # d['shape'] = df.shape[0]


def adjust_leverages():
    df = f_positions()
    df = df[~df['symbol'].isin(non_tradable)]
    df = df[df['leverage'] != leverage]
    symbols = df['symbol'].tolist()
    for s in symbols:
        client.futures_change_leverage(symbol=s, leverage=leverage)
        msg = 'leverage adjustment: {}'.format(s)
        logger.info(msg)
    return


def adjust_prices(d):
    if d['req'] > 0:
        return min(d['enterPrice'], d['lastPrice'])
    elif d['req'] < 0:
        return max(d['enterPrice'], d['lastPrice'])
    else:
        return d['lastPrice']

def get_balance():
    d = client.futures_position_information()
    print(d.keys())



    return


def trade():
    """
    - get current master data
    - get current prices
    - req = 0 or None, just update with current price
    - req = 1,
    :return:
    """

    d_m = read_json(fname_master)

    df_s = f_positions()[['symbol', 'entryPrice', 'positionAmt', 'unRealizedProfit']]
    d_s = df_s[df_s['entryPrice'] > 0].set_index('symbol').T.to_dict()
    print(d_s)

    d_p = latest_prices().set_index('symbol').T.to_dict()
    print(d_p)

    print('-'*25)
    get_balance()
    # for s, v in d_m.items():
    #     if 'req' in v and v['req'] == v['hlv'] and s not in non_tradable:
    #         print('* new request')
    #         if s in d_s: # if already in position
    #
    #         else:
    #             # Trailing price or place order
    #         print(s, v)
    #     else:
    #         # check consistency of current position
    #         if s in d_s: # we have position
    #             if v['hlv'] > 0 and d_s[s]['positionAmt'] < 0: # uptrend but we are short
    #                 print('uptrend but we are short')
    #                 print(s, v['hlv'], d_s[s]['positionAmt'])
    #             elif v['hlv'] < 0 and d_s[s]['positionAmt'] > 0:  # downtrend but we are long
    #                 print('downtrend but we are long')
    #                 print(s, v['hlv'], d_s[s]['positionAmt'])
    #             else:
    #                 print('OK ', s, v['hlv'], d_s[s]['positionAmt'])



if __name__ == '__main__':
    trade()
