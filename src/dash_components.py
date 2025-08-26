from dash import Dash, callback, Input, Output, html, dcc, ctx, dash_table
import dash_bootstrap_components as dbc
from price_handler import Price_Handler
import datetime as dt
from functions import to_dt, to_ts
import pandas as pd

pc = Price_Handler()

region_selector = html.Div([ # id = 'region_selector_dd'
dcc.Dropdown(
    options = [
        {'label': 'New South Whales 🐋', 'value':'NSW1'},
        {'label': 'South Australia ', 'value':'SA1'},
        {'label': 'Victoria', 'value':'VIC1'},
        {'label': 'Queensland ♛', 'value':'QLD1'},
    ], 
    value=pc.region, 
    id = 'region_selector_dd'
),
html.Div(id = 'region_selector_out_text')
])

@callback ( ## update region selector on selection 
    Output('region_selector_out_text', 'children'),
    Input('region_selector_dd', 'value')
)
def update_out_text(value):
    pc.region = value
    return f'current value selected = {value}'

date_range_selector = html.Div([
    dcc.DatePickerRange(
        id = 'date_selection_tool',
        month_format='Do MMM YYYY',
        display_format = 'DD-MM-YYYY',
        min_date_allowed=dt.date(2010,1,1),
        max_date_allowed=dt.date.today(),
        initial_visible_month=to_dt(pc.start_dt_nem).date(),
        start_date=to_dt(pc.start_dt_nem).date(),
        end_date=to_dt(pc.end_dt_nem).date(),
    ),
    html.Div(id = 'dates_selected')
])

@callback(
    Output('dates_selected', 'children'),
    Input('date_selection_tool', 'start_date'),
    Input('date_selection_tool', 'end_date') )
def update_date_selection(start_date, end_date):
    if start_date is not None:
        pc.start_dt_nem = pd.Timestamp(start_date)
    if end_date is not None: 
        pc.end_dt_nem = pd.Timestamp(end_date)

    return f'current selected {pc.start_dt_nem.date()} - {pc.end_dt_nem.date()}'

get_data_buttons = html.Div([
    html.Button('get_data', id = 'get_data_button', n_clicks=0),
    html.Button('refresh_data', id = 'refresh_data_button'),
    html.Div(id = 'data_get_status', children='waiting ... no!')
])

@callback (
    Output('price_agg_layout', 'children'),
    Input('get_data_button','n_clicks'),
    Input('refresh_data_button','n_clicks'),
    prevent_initial_call=True
)
def on_click(get_data, refresh_data):#, refresh_data):
    if pc.is_updating:
        return "wait I'm updating...", 'still upd'
    
    if ctx.triggered_id == 'get_data_button':
        pc.is_updating = True
        pc.get_prices()
        pc.is_updating = False

    elif ctx.triggered_id == 'refresh_data_button':
        pc.is_updating = True
        pc.get_prices(refresh=True)
        pc.is_updating = False
    
    
    return price_agg_layout


init_layout = html.Div([
    html.Div(children='TITLE : I am a title'),
    region_selector,
    date_range_selector,
    get_data_buttons,
],id = 'init_layout')

price_agg_layout = html.Div(
    [html.Div('sup!')],
    id = 'price_agg_layout' 
)

# @callback(
#     Input('month_selector', 'value'),
#     Output('price_agg_display', 'children')
#     )
# def on_month_select(v):
#     pc.display_prices_agg_by_month(v)

#     return dash_table.DataTable(pc.prices_agg_for_disp.to_dict('records'),[{"name": i, "id": i} for i in pc.prices_agg_for_disp.columns])

# dbc.Container([ dbc.Label('this a table'),
#                 dcc.Slider(0,len(pc.slider_points)-1, marks = {i:v for i,v in enumerate(pc.slider_points)}, id = 'month_selector'),
#                 dash_table.DataTable(pc.prices_agg_for_disp.to_dict('records'),[{"name": i, "id": i} for i in pc.prices_agg_for_disp.columns], id='price_agg_display')
#                                                 ])




