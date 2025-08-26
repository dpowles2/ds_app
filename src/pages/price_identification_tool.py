from dash import Dash, callback, Input, Output, html, dcc, ctx, dash_table, register_page, ALL
import dash_bootstrap_components as dbc
from price_handler import Price_Handler
from call_me_maybe import OptiRunner
import datetime as dt
from functions import to_dt, to_ts
import pandas as pd

register_page(__name__)

pc = Price_Handler()
opt = OptiRunner()

region_selector = html.Div([ html.H5('Select region of interest:'),
dcc.Dropdown(
    options = [
        {'label': 'New South Whales 🐋', 'value':'NSW1'},
        {'label': 'South Australia ', 'value':'SA1'},
        {'label': 'Victoria', 'value':'VIC1'},
        {'label': 'Queensland ♛', 'value':'QLD1'},
    ], 
    value=pc.region, 
    id = 'region_selector_dd',
    clearable=False
),
])

@callback ( ## update region selector on selection 
    Input('region_selector_dd', 'value')
)
def update_out_text(value):
    pc.region = value
    return

date_range_selector = html.Div([html.H5('Select date range of interest:'),
    dcc.DatePickerRange(
        id = 'date_selection_tool',
        month_format='Do MMM YYYY',
        display_format = 'DD-MM-YYYY',
        min_date_allowed=dt.date(2010,1,1),
        max_date_allowed=dt.date.today(),
        initial_visible_month=to_dt(pc.end_dt_nem).date(),
        start_date=to_dt(pc.start_dt_nem).date(),
        end_date=to_dt(pc.end_dt_nem).date(),
    ),
    html.Br(),
    html.Br(),
])

@callback(
    Input('date_selection_tool', 'start_date'),
    Input('date_selection_tool', 'end_date') )
def update_date_selection(start_date, end_date):
    if start_date is not None:
        pc.start_dt_nem = pd.Timestamp(start_date)
    if end_date is not None: 
        pc.end_dt_nem = pd.Timestamp(end_date)


get_data_buttons = html.Div([
    html.Button('Get data', id = 'get_data_button', n_clicks=0),
    html.Button('Refresh data', id = 'refresh_data_button', n_clicks=0),
    html.Br(),
    html.Br()
])

@callback (
    Output('mo_selector', 'children'),
    Output('col_sorter', 'children'),
    Output('months_of_interest', 'children'),
    Input('get_data_button','n_clicks'),
    Input('refresh_data_button','n_clicks'),
    prevent_initial_call=True
)
def on_click(get_data, refresh_data):#, refresh_data):
    if pc.is_updating:
        return html.H3("wait I'm updating..."), "","","",""
    
    if ctx.triggered_id == 'get_data_button':
        pc.is_updating = True
        pc.get_prices()
        pc.is_updating = False

    elif ctx.triggered_id == 'refresh_data_button':
        pc.is_updating = True
        pc.get_prices(refresh=True)
        pc.is_updating = False
    
    df = pc.months_of_interest.reset_index(drop=True)
    ls = [c for c in pc.return_prices_for_display().columns.to_list() if c not in ['Day', 'Volitility']]
    return  (
            html.Div(dcc.Slider(0,len(pc.slider_points)-1,step=1, marks = {i:v for i,v in enumerate(pc.slider_points)}, id='month_selector')),
            dbc.Container([dbc.Row([
                html.Div(dcc.Dropdown(['volitile', 'variable_but_below_strike','flatter'], 'volitile', id='vol_selector', clearable=False),style={'width': '30%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div(dcc.Dropdown(ls, ls[0], id='col_selector', clearable=False),style={'width': '30%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div(dcc.Dropdown(['Asc', 'Desc'], 'Asc', id='asc_or_desc_selector', clearable=False),style={'width': '30%', 'display': 'inline-block', 'padding': '10px'}),
                ])]),
            html.Div([html.Button(j.strftime('%b %Y'), id={'type': 'mo_button', 'index':i}) for i,j in enumerate(df.Month.to_list())])
    )

col_sorter = html.Div(id='col_sorter')

months_of_interest = html.Div([''], id = 'months_of_interest')
@callback (
    Output('month_details', 'children'),
    Output('price_agg_table', 'children', allow_duplicate=True),
    Input({'type': 'mo_button', 'index':ALL}, 'n_clicks'),
    prevent_initial_call='initial_duplicate'
)
def show_month_details(n_clicks):
    print(ctx.triggered_id)
    i = ctx.triggered_id['index']
    cats = pc.months_of_interest.loc[i,'Categories'].replace("'",'').split(',')
    month = pc.months_of_interest.loc[i,'Month']
    divs = [html.H4( month.strftime('%b %Y') )]
    divs.extend([html.Div(c) for c in cats])

    pc.display_prices_agg_by_month2(month)
    df = get_df()

    return html.Div(divs), df
    

month_details = html.Div([''],id='month_details')


def get_df():
    df = pc.return_prices_for_display()
    return dash_table.DataTable(df.to_dict('records'),[{"name": c, "id": c} for c in df.columns], style_table={'height': '300px', 'overflowY': 'auto'}, tooltip_data=[pc.tooltips], id='date_selector')

mo_selector = html.Div([''], id = 'mo_selector')
@callback (
    Output('price_agg_table', 'children', allow_duplicate=True),
    Input('month_selector', 'value'),
    prevent_initial_call=True
)
def on_month_select(v):
    pc.display_prices_agg_by_month(v)
    return get_df()


@callback (
    Output('price_agg_table', 'children', allow_duplicate=True),
    Input('asc_or_desc_selector', 'value'),
    prevent_initial_call=True
)
def on_asc_select(value):
    pc.asc = value == 'Asc'
    pc.display_prices_agg_by_sort()
    return get_df()


@callback (
    Output('price_agg_table', 'children', allow_duplicate=True),
    Input('col_selector', 'value'),
    prevent_initial_call=True
)
def sort_by_col(value):
    pc.col = pc.dict_pretty[value]
    pc.display_prices_agg_by_sort()
    return get_df()



@callback (
    Output('price_agg_table', 'children', allow_duplicate=True),
    Input('vol_selector', 'value'),
    prevent_initial_call=True
)
def select_vol(value):
    pc.vol = value
    pc.display_prices_agg_by_sort()
    return get_df()

price_agg_table = html.Div([''], id = 'price_agg_table')

@callback (
    Output('selected_value_plot', 'children'),
    Output('opti_button', 'children'),
    Input('date_selector', 'active_cell'),
    prevent_initial_call=True
)
def plot_selected_date(val):
    row = 0 if val is None else val['row']
    fig = pc.get_plot_data(row)
    return dcc.Graph(figure = fig), html.Button('Press Me', id='optimise on click')
    

selected_value_plot = html.Div([''], id = 'selected_value_plot')
optimise_button = html.Div([''], id = 'opti_button')

@callback (
    Output('optimiser_result', 'children'),
    Input('opti_button', 'n_clicks')
)
def optimise(n_clicks):
    if n_clicks:
        if n_clicks > 0:
            if pc.current_day is not None:
                return dcc.Graph(figure = opt.do_opti_run(pc.current_day,pc.region))

optimiser_result = html.Div(id='optimiser_result')

layout = html.Div([
    html.Div([html.H2('Price Identification Tool')]),
    region_selector,
    date_range_selector,
    get_data_buttons,
    months_of_interest,
    html.Br(),
    month_details,
    html.Br(),
    price_agg_table,
    mo_selector,
    col_sorter,
    selected_value_plot,
    optimise_button,
    optimiser_result
])