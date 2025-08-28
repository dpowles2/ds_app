import pandas as pd
import numpy as np
from utils.kusto_connection import kc
from price_identification_tool.functions import get_rmse, find_spread_n_hours, get_high_price_scores, cat_me
from os.path import exists
import plotly.express as px

class Price_Handler:
    def __init__(self):
        self.path = '.\\csvs'
        self.region = 'NSW1'
        self.start_dt_nem = pd.Timestamp('2021-10-01 00:00:00') # NEM
        self.end_dt_nem = pd.Timestamp.now()
        self.high_price_threshold = 1000
        self.med_price_threshold = 290
        self.prices = None
        self.prices_agg = None
        self.prices_agg_for_disp = None
        self.months_of_interest = None
        self.is_updating = False
        self.slider_points = None
        self.dates = None
        self.refresh_slider()
        self.current_day = None
        self.asc = True
        self.col = '_day'
        self.vol = 'volitile'
        self.pretty_dict = {i:j for i,j in zip(
        ['_day', 'volitility', f'dp_above_{self.high_price_threshold}_score','absolute_dp_score',f'dp_above_{self.med_price_threshold}_score', 'spread', 'rmse_p5_quantile', 'rmse_pd_quantile', 'max_period_score_p5_quantile', 'min_period_score_p5_quantile', 'max_period_score_pd_quantile', 'min_period_score_pd_quantile'],
        ['Day', 'Volitility', f'DP > {self.high_price_threshold} score','Absolute DP score',f'DP > {self.med_price_threshold} score', 'Spread', 'P5 RMSE', 'PD RMSE', 'P5 max spread alignment', 'P5 min spread alignment', 'PD max spread alignment', 'PD min spread alignment']
    )}
        self.dict_pretty = {j:i for i,j in self.pretty_dict.items()}
        self.tt_str = '0-1\ncompared with days of similar volitility\n(High is bad)'
        self.tooltips = {i:j for i,j in zip(
            ['Day', 'Volitility', f'DP > {self.high_price_threshold} score','Absolute DP score',f'DP > {self.med_price_threshold} score', 'Spread', 'P5 RMSE', 'PD RMSE', 'P5 max spread alignment', 'P5 min spread alignment', 'PD max spread alignment', 'PD min spread alignment'],
            ['Day', 'Volitility', f'Magnitude and number of prices above {self.high_price_threshold}', 'Sum of Absolute RRP / 1000', f'Magnitude and number of prices above {self.med_price_threshold}','Avg(Max(2 hours RRP)) - Avg(Min(2 hours RRP))',
             f'P5 RMSE Quantile Position {self.tt_str}', f'P5 RMSE Quantile Position {self.tt_str}', 
             f'P5-DP max-price spread alignment {self.tt_str}', f'P5-DP min-price spread alignment {self.tt_str}', 
             f'PD-DP max-price spread alignment {self.tt_str}', f'PD-DP min-price spread alignment {self.tt_str}']
        )}

    def get_prices(self, refresh = False):
        path = f'{self.path}\\prices_{self.region}.json'
        if exists(path) and not refresh:
            print('loading existing prices')
            self.prices = pd.read_json(path)
            self.prices.REGIONID = self.prices.REGIONID.fillna(self.region)
            self.prices.interval_start = pd.to_datetime(self.prices.interval_start, unit='ms')
            self.prices._day = pd.to_datetime(self.prices._day, unit='ms')
        else:
            print('getting prices')
            self.prices = self.get_prices_for_region()
            self.prices.to_json(path, index = None)
        print('begin price agg')
        self.get_prices_agg(refresh)
        self.get_months_of_interest()
        print('showing')
        self.display_prices_agg_by_month()
        self.refresh_slider()

    def get_prices_for_region(self):
        start_dt_nem, end_dt_nem, region = self.start_dt_nem, self.end_dt_nem, self.region
        
        db = 'BtmoInfoserver'
        q = f'''set notruncation;
        let time_range = range interval_start from datetime({start_dt_nem}) to datetime({end_dt_nem}) step 5m
        | extend _day = startofday(interval_start);
        time_range
        | join kind=leftouter
        (
        DISPATCHPRICE_ADF
        | extend interval_start = SETTLEMENTDATE - 5m
        | where interval_start between (datetime({start_dt_nem}) .. datetime({end_dt_nem}))
        | where REGIONID == '{region}'
        | project interval_start, REGIONID, RRP
        | summarize arg_max(ingestion_time(), *) by interval_start, REGIONID
        | order by interval_start asc
        | extend _day = startofday(interval_start)
        | extend DP_RRP = RRP
        | project-away RRP
        ) on interval_start
        | join kind=leftouter 
        (
        P5MIN_REGIONSOLUTION
        | extend interval_start = INTERVAL_DATETIME - 5m
        | where interval_start between (datetime({start_dt_nem}) .. datetime({end_dt_nem}))
        | where REGIONID == '{region}'
        | project interval_start, RRP, RUN_DATETIME
        | summarize arg_max(ingestion_time(), *) by interval_start, RUN_DATETIME
        | sort by interval_start, RUN_DATETIME asc
        | summarize P5_RRP = make_list(RRP) by interval_start
        ) on interval_start
        | project interval_start, _day, REGIONID, DP_RRP, P5_RRP
        | sort by interval_start asc
        '''

        prices = kc(db,q)
        prices = prices.sort_values('interval_start', ascending=True)
        prices.DP_RRP = prices.DP_RRP.ffill()
        # prices.DP_RRP = prices.DP_RRP.apply(lambda x: round(x,2))
        prices.P5_RRP = prices.P5_RRP.ffill()
        def fill_ls(ls,dp):
            delta = [abs(i-dp) for i in ls]
            v = ls[delta.index(max(delta))]
            while(len(ls) < 12):
                ls.append(v)
            return ls
        prices.P5_RRP = prices.apply(lambda x: x.P5_RRP if len(x.P5_RRP)== 12 else fill_ls(ls = x.P5_RRP,dp = x.DP_RRP), axis=1)

        q = f'''PREDISPATCHPRICE_ADF
        | extend interval_start = DATETIME - 30m
        | where interval_start between (datetime({start_dt_nem}) .. datetime({end_dt_nem}))
        | where REGIONID == '{region}'
        | project interval_start, RRP, LASTCHANGED
        | extend _day = startofday(interval_start)
        | where LASTCHANGED <= _day
        | summarize arg_max(LASTCHANGED, *) by interval_start
        | project interval_start, _day, PD_RRP = RRP
        | sort by interval_start asc
        | mv-expand minute_offset = range(0,25,5)
        | extend interval_start = interval_start + totimespan(strcat(minute_offset,'m'))
        | project-away minute_offset'''

        pd_prices = kc(db,q)
        prices = prices.merge(pd_prices[['interval_start', 'PD_RRP']], on = 'interval_start', how = 'left')
        prices.PD_RRP = prices.PD_RRP.fillna(0)
        prices.REGIONID = prices.REGIONID.fillna(self.region)

        return prices

    def get_prices_agg(self, refresh = False):
        path = f'{self.path}\\prices_agg_{self.region}.json'
        if exists(path) and not refresh:
            self.prices_agg = pd.read_json(path)
        else:
            self.prices_agg = self.prices.groupby('_day').agg({
                'REGIONID': 'max',
                'DP_RRP': (lambda x: list(x)),
                'P5_RRP': (lambda x: list(x)),
                'PD_RRP': (lambda x: list(x)),
            }
            ).reset_index()
            self.compute_rating_metrics()
            self.prices_agg._day = pd.to_datetime(self.prices_agg._day, unit='ms')
            self.prices_agg['Month'] = self.prices_agg._day.apply(lambda x: x.replace(day=1))
            self.prices_agg.to_json(path, index = None)
            self.prices_agg = pd.read_json(path)   

    def compute_rating_metrics(self):
        self.prices_agg.DP_RRP = self.prices_agg.DP_RRP.apply(lambda x: np.array(x))
        self.prices_agg.P5_RRP = self.prices_agg.P5_RRP.apply(lambda x: np.array(x))
        self.prices_agg.PD_RRP = self.prices_agg.PD_RRP.apply(lambda x: np.array(x))

        self.prices_agg['rmse_p5'] = self.prices_agg.apply(lambda x: get_rmse(x.DP_RRP, x.P5_RRP), axis=1)
        self.prices_agg['rmse_p5'] = self.prices_agg.rmse_p5.apply(lambda x: x.sum()/x.shape[0])

        self.prices_agg['rmse_pd'] = self.prices_agg.apply(lambda x: get_rmse(x.DP_RRP, x.PD_RRP), axis=1)
        self.prices_agg['rmse_pd'] = self.prices_agg.rmse_pd.apply(lambda x: x.sum()/x.shape[0])

        self.prices_agg[['max_period_score_p5','min_period_score_p5', 'spread']] = self.prices_agg.apply(lambda x: find_spread_n_hours(x.DP_RRP, x.P5_RRP), axis = 1)
        self.prices_agg[['max_period_score_pd','min_period_score_pd', 'spread']] = self.prices_agg.apply(lambda x: find_spread_n_hours(x.DP_RRP, x.PD_RRP), axis = 1)

        self.prices_agg[[f'dp_above_{self.high_price_threshold}_score','absolute_dp_score',f'dp_above_{self.med_price_threshold}_score']] = self.prices_agg.DP_RRP.apply(
            lambda x: get_high_price_scores(x, self.high_price_threshold, self.med_price_threshold) )
        ## TODO configureable
        hp_threshold = 3
        quantile = 0.7
        medium_price_thresholds = [
            np.quantile(self.prices_agg.absolute_dp_score.to_numpy(), [quantile]),
            np.quantile(self.prices_agg[f'dp_above_{self.med_price_threshold}_score'].to_numpy(), [quantile])
        ]

        self.prices_agg['volitility'] = self.prices_agg.apply(lambda x: cat_me(x[f'dp_above_{self.high_price_threshold}_score'], x['absolute_dp_score'], x[f'dp_above_{self.med_price_threshold}_score'], hp_threshold, medium_price_thresholds),axis=1)

        cols = ['rmse_p5', 'rmse_pd',
       'max_period_score_p5', 'min_period_score_p5', 
       'spread',
       'max_period_score_pd', 'min_period_score_pd']

        thresholds = self.prices_agg.groupby('volitility').agg({
            c:(lambda x: list(x))
            for c in cols
            }).reset_index()

        def get_quantile_position_for_scoring_metric(sc, v, score):
            scores =  thresholds[thresholds.volitility == v][score].to_list()[0]
            return (scores<sc).mean()

        for c in cols:
            thresholds[c] = thresholds[c].apply(lambda x: np.array(x))
            self.prices_agg[f'{c}_quantile'] = self.prices_agg.apply(lambda x: get_quantile_position_for_scoring_metric(x[c], x.volitility, score=c), axis = 1)
        ## TODO configureable
        low = 0.2
        high = 0.8

        for c in cols:
            if c == 'spread':
                self.prices_agg[f'{c}_high'] = self.prices_agg[f'{c}_quantile'].apply(lambda x: x>high)
                self.prices_agg[f'{c}_low'] = self.prices_agg[f'{c}_quantile'].apply(lambda x: x<low)
            else:
                self.prices_agg[f'{c}_inaccurate'] = self.prices_agg[f'{c}_quantile'].apply(lambda x: x>high)
                self.prices_agg[f'{c}_accurate'] = self.prices_agg[f'{c}_quantile'].apply(lambda x: x<low)
        
        self.prices_agg[['month', 'year']] = self.prices_agg.apply(lambda x: pd.Series({'month':x._day.month,'year':x._day.year}), axis=1)

    def display_prices_agg_by_month(self, date_ind = None):
        if date_ind is None:
            month = self.start_dt_nem.month
            year = self.start_dt_nem.year

        else:
            month = self.dates[date_ind].month
            year = self.dates[date_ind].year

        self.prices_agg_for_disp = self.prices_agg[(self.prices_agg.month == month) & (self.prices_agg.year == year)].sort_values('_day').reset_index(drop=True)
        self.prices_agg_for_disp._day = pd.to_datetime(self.prices_agg_for_disp._day, unit='ms')
        self.prices_agg_for_disp._day = self.prices_agg_for_disp._day.apply(lambda x: x.date() )

    def display_prices_agg_by_month2(self, date):

        month = date.month
        year = date.year

        self.prices_agg_for_disp = self.prices_agg[(self.prices_agg.month == month) & (self.prices_agg.year == year)].sort_values('_day').reset_index(drop=True)
        self.prices_agg_for_disp._day = pd.to_datetime(self.prices_agg_for_disp._day, unit='ms')
        self.prices_agg_for_disp._day = self.prices_agg_for_disp._day.apply(lambda x: x.date() )

    def display_prices_agg_by_sort(self):
        self.prices_agg_for_disp = self.prices_agg[self.prices_agg.volitility == self.vol]
        self.prices_agg_for_disp = self.prices_agg_for_disp.sort_values(self.col, ascending=self.asc).reset_index(drop=True).iloc[:30]
        self.prices_agg_for_disp._day = pd.to_datetime(self.prices_agg_for_disp._day, unit='ms')
        self.prices_agg_for_disp._day = self.prices_agg_for_disp._day.apply(lambda x: x.date() )
    
    def return_prices_for_display(self):
        cols = [
            '_day', 'volitility', 
            f'dp_above_{self.high_price_threshold}_score','absolute_dp_score',f'dp_above_{self.med_price_threshold}_score', 'spread',
            'rmse_p5_quantile', 'rmse_pd_quantile', 
            'max_period_score_p5_quantile', 'min_period_score_p5_quantile',
            'max_period_score_pd_quantile', 'min_period_score_pd_quantile'
            ]
        df = self.prices_agg_for_disp[cols]
        
        for c in cols: 
            if c in ['_day', 'volitility']: continue
            df[c] = df[c].apply(lambda x: round(x,2))
        
        df.columns = [self.pretty_dict[c] for c in cols]

        return df
    
    def refresh_slider(self):
        self.dates = pd.date_range(start=self.start_dt_nem, end=self.end_dt_nem, freq='MS')
        self.slider_points = [i.strftime('%Y %b') if (i.strftime('%b') == 'Jan' or n == 0)  else i.strftime('%b') for n,i in enumerate(self.dates)]        

    def get_plot_data(self, row):
        row = 0 if row is None else row
        day = self.prices_agg_for_disp.loc[row,'_day'] 
        self.current_day = day
        dp = np.array(self.prices_agg_for_disp.loc[row,'DP_RRP'])
        pd_ = np.array(self.prices_agg_for_disp.loc[row,'PD_RRP'])
        p5 = np.array(self.prices_agg_for_disp.loc[row,'P5_RRP']).mean(axis=1)
        times = [x.time() for x in pd.date_range(start = '00:00:00',end='23:55:00',freq = '5min')]
        df = pd.DataFrame({'time':times,'dp':dp, 'p5':p5,'pd':pd_}).melt(id_vars='time', value_vars=['dp','p5','pd'])

        fig = px.line(df,x='time',y='value', color='variable')
        # xaxis=dict(tickformat="%HH",dtick=3_600_000)
        title = str(day)
        fig.update_layout(
            # xaxis=xaxis, 
            title=title
            )
        return fig
    
    def get_months_for_cat(self, cat,val, m,c, n=2, vol = False):
        v_cats = ['volitile', 'variable_but_below_strike', 'flatter']
        if vol:
            if not isinstance(n,list):
                n = [n for i in v_cats]
            for i,v in enumerate(v_cats):
                if n[i] == 0:
                    continue
                temp = self.prices_agg[self.prices_agg.volitility == v].groupby('Month')[[val]].sum().sort_values(val, ascending=False).reset_index()
                m.extend(temp.iloc[:n[i]].Month.to_list())
                c.extend([v+' '+cat]*n[i])
        else:
            temp = self.prices_agg.groupby('Month')[[val]].sum().sort_values(val, ascending=False).reset_index()
            m.extend(temp.iloc[:n].Month.to_list())
            c.extend([cat]*n)
        return m,c
    
    def get_finessed_months_for_cat(self):
        vol_max = self.prices_agg[self.prices_agg.volitility == 'volitile'].sort_values('dp_above_1000_score', ascending=False).reset_index(drop=True)
        vol_min = self.prices_agg[self.prices_agg.volitility == 'volitile'].sort_values('dp_above_1000_score', ascending=True).reset_index(drop=True)
        sh = int(vol_max.shape[0] * 0.2)
        vol_max = vol_max.iloc[:sh]
        vol_min = vol_min.iloc[:sh]

        var_max = self.prices_agg[self.prices_agg.volitility == 'variable_but_below_strike'].sort_values('absolute_dp_score', ascending=False).reset_index(drop=True)
        var_min = self.prices_agg[self.prices_agg.volitility == 'variable_but_below_strike'].sort_values('absolute_dp_score', ascending=True).reset_index(drop=True)
        sh = int(var_max.shape[0] * 0.2)
        var_max = var_max.iloc[:sh]
        var_min = var_min.iloc[:sh]

        return [vol_max,vol_min,var_max,var_min]

    def compute_with_extremes(self, dfs, val, m,c, n=2):
        vol_max,vol_min,var_max,var_min = dfs

        ## VOL
        cat = 'vol_max by highest' + val
        temp = vol_max.groupby('Month')[[val]].count().sort_values(val, ascending=False).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        cat = 'vol_max by lowest' + val
        temp = vol_max.groupby('Month')[[val]].count().sort_values(val, ascending=True).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        cat = 'vol_min by highest' + val
        temp = vol_min.groupby('Month')[[val]].count().sort_values(val, ascending=False).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        cat = 'vol_min by lowest' + val
        temp = vol_min.groupby('Month')[[val]].count().sort_values(val, ascending=True).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)
        ## VAR
        cat = 'var by highest' + val
        temp = var_max.groupby('Month')[[val]].count().sort_values(val, ascending=False).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        cat = 'var_max by lowest' + val
        temp = var_max.groupby('Month')[[val]].count().sort_values(val, ascending=True).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        cat = 'var_min by highest' + val
        temp = var_min.groupby('Month')[[val]].count().sort_values(val, ascending=False).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        cat = 'var_min by lowest' + val
        temp = var_min.groupby('Month')[[val]].count().sort_values(val, ascending=True).reset_index()
        m.extend(temp.iloc[:n].Month.to_list())
        c.extend([cat]*n)

        return m,c


    def get_months_of_interest(self):
        month, cats = [],[]

        cat = 'highest high price months'
        val = 'dp_above_1000_score'
        month, cats = self.get_months_for_cat(cat,val,month,cats)

        cat = 'highest abs price months'
        val = 'absolute_dp_score'
        month, cats = self.get_months_for_cat(cat,val,month,cats)

        cat = 'highest spread months'
        val = 'spread'
        month, cats = self.get_months_for_cat(cat,val,month,cats)

        ## High low spread
        cat = 'high spread'
        val = 'spread_high'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'low spread'
        val = 'spread_low'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        ## RMSE ACC
        cat = 'accurate p5 rmse'
        val = 'rmse_p5_accurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'inaccurate p5 rmse'
        val = 'rmse_p5_inaccurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'accurate pd rmse'
        val = 'rmse_pd_accurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'inaccurate pd rmse'
        val = 'rmse_pd_inaccurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        ## SPREAD ACC
        cat = 'accurate p5 spread period'
        val = 'max_period_score_p5_accurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'inaccurate p5 spread period'
        val = 'max_period_score_p5_inaccurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'accurate pd spread period'
        val = 'max_period_score_pd_accurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        cat = 'inaccurate pd spread period'
        val = 'max_period_score_pd_inaccurate'
        month, cats = self.get_months_for_cat(cat,val,month,cats, n = [2,2,1], vol=True)

        # dfs = self.get_finessed_months_for_cat()

        ## High low spread
        # val = 'spread'
        # month, cats = self.compute_with_extremes(dfs,val,month,cats)


        self.months_of_interest = pd.DataFrame({'Month': month, 'Categories':cats}).groupby('Month').agg(lambda x: str(list(x))[1:-1] ).reset_index()
        if isinstance(self.months_of_interest.loc[0,'Month'],np.int64):
            self.months_of_interest.Month = pd.to_datetime(self.months_of_interest.Month, unit='ms')
            self.months_of_interest.Month = self.months_of_interest.Month.apply(lambda x: x.date())

    