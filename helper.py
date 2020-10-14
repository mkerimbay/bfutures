import pandas as pd
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 500)
import numpy as np
from datetime import datetime as dt
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


def last_orderId(table):
    q = 'select * from ' + table + ' order by rowid desc limit 1'
    cur.execute(q)
    df = pd.read_sql(q, con)
    df.time = pd.to_datetime(df.time)
    return df['id'].values[0], df['orderId'].values[0]


def get_data_sql(s):
    # try:
    q = 'select * from {}'.format(s)
    return pd.read_sql(q, con)
    # except Exception as e:
    #     logger.exception(e)
    #     return pd.DataFrame()


def store_trades():
    table_name = 'Trades'
    df = pd.DataFrame(client.futures_account_trades())
    df.time = pd.to_datetime(df.time, unit='ms')
    df['id'] = df['id'].astype(str)
    df['orderId'] = df['orderId'].astype(str)
    df['price'] = df['price'].astype(float)
    df['qty'] = df['qty'].astype(float)
    df['realizedPnl'] = df['realizedPnl'].astype(float)
    df['quoteQty'] = df['quoteQty'].astype(float)
    df['commission'] = df['commission'].astype(float)
    if table_exists(table_name):
        _id, _order = last_orderId(table_name)
        print(_id, _order)
        try:
            ind = df[(df['id'] == _id) & (df['orderId'] == _order)].index[0]
            df = df[ind+1:]
            if df.shape[0] > 0:
                df.to_sql(table_name, con, index=None, if_exists='append')
                msg = '** Trades table updated with {} records'.format(df.shape[0])
                logger.info(msg)
        except:
            df.to_sql(table_name, con, index=None, if_exists='append')
            msg = '* error getting last trade id from sql, appending all..'
            logger.info(msg)
    else:
        df.to_sql(table_name, con, index=None)
        msg = '** Trades table created with {} records'.format(df.shape[0])
        logger.info(msg)


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
    try:
        try:
            kl = client.get_klines(symbol=s, interval=interval)
        except:
            kl = client.futures_klines(symbol=s, interval=interval)
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
        msg = 'cant handle {}'.format(s)
        logger.info(msg)
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
        df['lastPrice'] = df['lastPrice'].astype(float)
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
                        reqPrice = df.tail(1)['close'].values[0]
                        d['reqPrice'] = reqPrice
                        msg = '** Trend change : {}, {} , {}'.format(s, last_hlv, reqPrice)
                        logger.info(msg)
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

def available_usd():
    d = client.futures_account()['assets']
    for each in d:
        if each['asset'] == 'USDT':
            return float(each['maxWithdrawAmount'])
    return 0


def get_amount(s, p, l):
    denom = read_json(fname_denom)[s]
    dQty = denom['dQty']
    dPrice = denom['dPrice']
    return round(trade_amnt*l/p, dQty)


def change_short(a,b):
    return round((a/b-1)*100,2)


def trade():
    """
    - get current master data
    - get current prices
    - req = 0 or None, just update with current price
    - req = 1,
    :return:
    """

    d_m = read_json(fname_master)
    d_req = {}
    for k,v in d_m.items():
        if 'req' in v:
            d_req[k] = v
    print(d_req)

    if len(d_req) == 0: # no requests
        logger.info('no request, exit')
        return

    df_s = f_positions()[['symbol', 'entryPrice', 'positionAmt', 'unRealizedProfit', 'leverage']]
    d_leverages = df_s[['symbol', 'leverage']].set_index('symbol').T.to_dict()
    d_position = df_s[df_s['entryPrice'] > 0].set_index('symbol').T.to_dict()
    print(d_position)

    d_prices = latest_prices().set_index('symbol').T.to_dict()
    print(d_prices)

    print('-'*25)
    usd_available = available_usd()

    for s,v in d_req.items():
        req = v['req']

        # update req prices
        if req == 1:
            d_req[s]['reqPrice'] = min(d_req[s]['reqPrice'], d_prices[s]['lastPrice'])
        else:
            d_req[s]['reqPrice'] = max(d_req[s]['reqPrice'], d_prices[s]['lastPrice'])

        print('*'*25)
        print(s)
        if s in non_tradable:
            msg = '** Non tradable : {}, clearing request'.format(s)
            logger.info(msg)
            del d_req[s]['req']
            del d_req[s]['reqPrice']
        else:
            if req == 1:  # LONG
                if s in d_position and d_position[s]['positionAmt'] > 0: # do we have long position?
                    msg = '** Already Long for: {}, clearing request'.format(s)
                    logger.info(msg)
                    del d_req[s]['req'] # clear request
                    del d_req[s]['reqPrice']
                else:
                    if d_prices[s]['lastPrice'] > (v['reqPrice'] * (1+safety_percent/100)): # price is more than 2%
                        if usd_available < 2*trade_amnt: # if no balance, clear request
                            msg = '** No balance to buy : {}, clearing request'.format(s)
                            logger.info(msg)
                            del d_req[s]['req']  # clear request
                            del d_req[s]['reqPrice']
                        else:
                            print('proceed to order long')
                            # first close short position
                            amnt1 = get_amount(s, d_prices[s]['lastPrice'], d_leverages[s]['leverage'])
                            amnt2 = 0
                            if s in d_position and d_position[s]['positionAmt'] < 0:
                                amnt2 = abs(d_position[s]['positionAmt'])
                                msg = '** Closing {} short position... entered: {} , closed: {}, amount: {}, Profit: {}$, change : {}%'.format(s, d_position[s]['entryPrice'], d_prices[s]['lastPrice'], amnt2, d_position[s]['unRealizedProfit'], change_short(d_position[s]['entryPrice'],d_prices[s]['lastPrice'] ))
                                logger.info(msg)
                                # calculate profit/loss, log, telegram

                            # place long order
                            client.futures_create_order(symbol=s, side='BUY', type='MARKET', quantity=amnt1+amnt2)
                            msg = '** Placing long order: {} for amount of {} at price: {}'.format(s, amnt1, d_prices[s]['lastPrice'])
                            logger.info(msg)
                            # notify, log, telegram
                            del d_req[s]['req']  # clear request
                            del d_req[s]['reqPrice']
                    else:
                        msg = '{} waiting to buy, req: {}, curr: {}'.format(s, d_req[s]['reqPrice'], d_prices[s]['lastPrice'])
                        logger.info(msg)
            else: #SHORT
                if s in d_position and d_position[s]['positionAmt'] < 0: # do we have short position?
                    msg = '** Already Short for: {}, clearing request'.format(s)
                    logger.info(msg)
                    del d_req[s]['req'] # clear request
                    del d_req[s]['reqPrice']
                else:
                    if d_prices[s]['lastPrice'] < (v['reqPrice'] * (1-safety_percent/100)): # price is less than 2%
                        if usd_available < 2*trade_amnt: # if no balance, clear request
                            msg = '** No balance to buy : {}, clearing request'.format(s)
                            logger.info(msg)
                            del d_req[s]['req']  # clear request
                            del d_req[s]['reqPrice']
                        else:
                            print('proceed to order short')
                            # first close open position
                            amnt1 = get_amount(s, d_prices[s]['lastPrice'], d_leverages[s]['leverage'])
                            amnt2 = 0
                            if s in d_position and d_position[s]['positionAmt'] > 0:
                                amnt2 = abs(d_position[s]['positionAmt'])
                                msg = '** Closing {} long position... entered: {} , closed: {}, amount: {}, Profit: {}$, change : {}%'.format(s, d_position[s]['entryPrice'], d_prices[s]['lastPrice'], amnt2, d_position[s]['unRealizedProfit'], change_short(d_prices[s]['lastPrice'], d_position[s]['entryPrice']))
                                logger.info(msg)
                                # calculate profit/loss, log, telegram

                            # place Short order
                            client.futures_create_order(symbol=s, side='SELL', type='MARKET', quantity=amnt1+amnt2)
                            msg = '** Placing Short order: {} for amount of {} at price : {}'.format(s, amnt1, d_prices[s]['lastPrice'])
                            logger.info(msg)
                            # notify, log, telegram
                            del d_req[s]['req']  # clear request
                            del d_req[s]['reqPrice']
                    else:
                        msg = '{} waiting to sell, req: {}, curr: {}'.format(s, d_req[s]['reqPrice'], d_prices[s]['lastPrice'])
                        logger.info(msg)
                        # do nothing, just wait

    for k,v in d_req.items():
        d_m[k] = v
    json.dump(d_m, open(fname_master, 'w+'))

    print(d_req)


def snapshot():
    try:
        d = client.futures_account()
        df = pd.DataFrame(d['assets'])
        df = df[df['asset'] == 'USDT']
        df['n_assets'] = len(d['positions'])
        df['n_positions'] = len([x for x in d['positions'] if float(x['entryPrice']) > 0])
        df['time'] = dt.now().strftime("%Y-%m-%d %H:%M")
        df.to_sql('snap_USDT', con, index=None, if_exists='append')
        msg = '* snapshot taken'
        logger.info(msg)
    except Exception as e:
        logger.exception(e)



if __name__ == '__main__':
    snapshot()
