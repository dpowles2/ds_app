import numpy as np
import pandas as pd
import datetime as dt

def get_rmse(dp,pd_):
    dim = dp.shape[0]
    dp = dp.reshape((dim,1)) if len(dp.shape) == 1 else dp
    pd_ = pd_.reshape((dim,1)) if len(pd_.shape) == 1 else pd_
    return (((dp-pd_)**2).sum(axis=1)/pd_.shape[1])**0.5

def find_magnitude(dp,pd_, plotme=False):
    # if price is p5 look at the average p5 if its pd then leave as is
    dim = dp.shape[0]
    if len(pd_.shape) > 1:
        pd_ = pd_.mean(axis=1)
        
    dp_mag, pd_mag = np.argsort(dp), np.argsort(pd_)

    return dp_mag, pd_mag

# find where PD accurately predicts high and low price times 
def find_spread_n_hours(dp,pd_, n = 2):
    dim = dp.shape[0]
    max_segs = dim - n * 12
    min_segs = n*12 - 1

    dp_mag, pd_mag = find_magnitude(dp,pd_)

    dp_max_times = dp_mag[max_segs:]
    pd_max_times = pd_mag[max_segs:]
    temp_a = np.sort(np.array([i for i in dp_max_times if i not in pd_max_times]))
    temp_b = np.sort(np.array([i for i in pd_max_times if i not in dp_max_times]))
    max_delta = ((temp_a - temp_b)**2).sum()**0.5

    dp_min_times = dp_mag[:min_segs]
    pd_min_times = pd_mag[:min_segs]
    temp_a = np.sort(np.array([i for i in dp_min_times if i not in pd_min_times]))
    temp_b = np.sort(np.array([i for i in pd_min_times if i not in dp_min_times]))
    min_delta = ((temp_a - temp_b)**2).sum()**0.5

    spread = dp[dp_max_times].mean() - dp[dp_min_times].mean()

    return pd.Series( {'max_period_score': max_delta, 'min_period_score': min_delta, 'spread': spread} )


def get_high_price_scores(dp, high_price_threshold, med_price_threshold):
    out = {
        f'dp_above_{high_price_threshold}_score': (abs(dp)//high_price_threshold).sum(),
        'absolute_dp_score': ((abs(dp)).sum()/1000),
        f'dp_above_{med_price_threshold}_score': (abs(dp)//290).sum(),
    }
    return pd.Series(out)


cats = ['volitile', 'variable_but_below_strike','flatter']

def cat_me(hps, hps2, hps3, hp_threshold, medium_price_threshold):

    if hps >=hp_threshold: return cats[0]
    elif hps2 >= medium_price_threshold[0]: return cats[1]
    elif hps3 >= medium_price_threshold[1]: return cats[1]
    else: return cats[2]

def to_ts(_dt):
    if isinstance(_dt,dt.datetime):
        return(pd.Timestamp(year = _dt.year, month=_dt.month, day=_dt.day, hour = _dt.hour, minute=_dt.minute, second=_dt.second))
    elif isinstance(_dt, dt.date):
        return(pd.Timestamp(year = _dt.year, month=_dt.month, day=_dt.day))
    return _dt
def to_dt(ts):
    return(dt.datetime(year = ts.year, month=ts.month, day=ts.day, hour = ts.hour, minute=ts.minute, second=ts.second))