import pandas as pd
import datetime as dt
from dash import Dash, html, dcc, callback, Output, Input, register_page
import plotly.express as px
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from utils.kusto_connection import kc

register_page(__name__)

db = 'distributionbattery'

class State_Manager:
    def __init__(self):
        self.bess = None
        self.schedule = None
        self.price = None
        self.ls = ['power', 'soe']
    
    def update_state(self,s,e):
        q = f"""Optimizer_Data
        | where bessId == '{self.bess}'
        | where invocationTime + 10h between (startofday(datetime({str(s)})) .. endofday(datetime({str(e)})))
        | where invocationMode == 'ForecastTertiary'
        | order by ingestion_time() asc 
        | where name in ('Schedule', 'PriceForecast')
        | extend invocationTime = invocationTime + 10h
        | extend sod = startofday(invocationTime)"""

        out = kc(db, q)
        out.invocationTime = out.invocationTime.round('min')

        schedule = out[out.name == 'Schedule'][['invocationTime','value']].reset_index(drop=True)
        price = out[out.name == 'PriceForecast'][['invocationTime','value']].reset_index(drop=True)

        schedule['ts'] = schedule.value.apply(lambda x: x['data']['unix_timestamps'])
        schedule['ts1'] = schedule.ts.apply(lambda x: x[0])
        schedule['sch'] = schedule.value.apply(lambda x: x['data']['series']['energy'])
        schedule['soe'] = schedule.value.apply(lambda x: x['data']['series']['soe_at_interval_end'])
        schedule['cycles'] = schedule.value.apply(lambda x: x['data']['series']['ac_cycle_count_at_interval_end'])

        price['ts'] = price.value.apply(lambda x: x['data']['unix_timestamps'])
        key = [k for k in price.loc[0,'value']['data']['series'].keys() if 'WholesaleSpotPrice' in k][0]
        price['price'] = price.value.apply(lambda x: x['data']['series'][key])

        self.schedule = schedule.sort_values('invocationTime', ascending=True)
        self.price = price.sort_values('invocationTime', ascending=True)

current_state = State_Manager()

get_besses_q = '''Optimizer_Data | distinct bessId | where isnotempty(bessId)'''
besses = kc(db, get_besses_q).bessId.to_list()

@callback(
    Input('my_checkbox', 'value')
)
def clicky_click_click(checkbox_list):
    current_state.ls = checkbox_list


## call on selecting ID updates datepicker
@callback(
    Output('date_selector', 'children'),
    Input('bess_dropdown', 'value')
)
def update_output(value):
    q = f'''Optimizer_Data
    | where bessId == '{value}'
    | summarize max_ = max(invocationTime), min_ = min(invocationTime)'''
    current_state.bess = value
    df = kc(db,q)
    
    date_min = df.loc[0,'min_']
    date_max = df.loc[0,'max_']
    return dcc.DatePickerRange(
        id='date_picker_range',
        min_date_allowed=dt.date(date_min.year, date_min.month, date_min.day),
        max_date_allowed=dt.date(date_max.year, date_max.month, date_max.day),
        initial_visible_month=dt.date(date_min.year, date_min.month, date_min.day)
    )


## call on selecting a date, updates plot if both dates are picked than one date is pick
@callback(
    Output('live_update_graph', 'children'),
    Input('date_picker_range', 'start_date'),
    Input('date_picker_range', 'end_date')
    )
def use_dates(start_date, end_date):
    if start_date is None or end_date is None:
        return f"Pick a date any (valid) date!"
    
    current_state.update_state(start_date, end_date)
    return html.Div([
        dcc.Graph(id='live_update_graph'),
        dcc.Interval(id='interval_component', interval=100,n_intervals=0)
    ])

## updates plot once there is a plot
@callback(
    Output('live_update_graph', 'figure'),
    Input('interval_component', 'n_intervals')
)
def update_plot(i):
    if current_state.schedule is None:
        return
    i = i % (current_state.schedule.shape[0]-1)
    df = pd.DataFrame(
        {
            'datetime':current_state.schedule.loc[i,'ts'],
            'power':current_state.schedule.loc[i,'sch'],
            'soe':current_state.schedule.loc[i,'soe'],
            'cycles':current_state.schedule.loc[i,'cycles']
            })
    m = df.soe.max()
    df.cycles = df.cycles * m
    value_vars = current_state.ls
    if 'price' in value_vars:
        temp = pd.DataFrame(
            {
                'datetime':current_state.price.loc[i,'ts'],
                'price':current_state.price.loc[i,'price']
                })

        df = df.merge(temp, on='datetime', how='left')
        df = df.sort_values('datetime',ascending=True).reset_index(drop=True)

        for j in range(df.shape[0]):
            df.loc[j,'price'] = df.loc[j,'price'] if not pd.isna(df.loc[j,'price']) else df.loc[j-1,'price']

        max_p = df.price.max()
        max_soe = df.soe.max()
        df.price = df.price.apply(lambda x: x/max_p * max_soe)
    df.datetime = df.datetime.apply(lambda x: pd.Timestamp.fromtimestamp(int(x)))
    df = df.melt(id_vars='datetime', value_vars=value_vars)
    fig = px.line(df,x='datetime',y='value', color='variable')
    xaxis=dict(tickformat="%d/%m\n%H:00",dtick=3_600_000)
    title = "horizon starting: " + str(df.loc[0,'datetime'])
    fig.update_layout(xaxis=xaxis, title=title)

    return fig

layout = html.Div([ 
    html.Div([dcc.Dropdown(besses, besses[0], id='bess_dropdown')]),
    html.Div(id='date_selector'),
    html.Div(id='live_update_graph'),
    html.Div(
        [dcc.Checklist(
        id='my_checkbox',
        options=[{'label': 'Power', 'value': 'power'},{'label': 'SoE', 'value': 'soe'},{'label': 'Cycles', 'value': 'cycles'},{'label': 'Price', 'value': 'price'}],
        value=[],  # Default to unchecked (False)
        labelStyle={'display': 'inline-block'}
    )]
    )
])