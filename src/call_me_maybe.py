import requests
import json 
import pandas as pd
import datetime as dt
from classes import (
    BessCharacteristics,
    BessState,
    MarketType,
    BessId,
    CyclingPenalty,
    Bess,
    Timeseries,
    TimeseriesData,
    InputData,
    InvocationRequest,
    InvocationMode,
    LimitTypes,
    NetworkLimit, 
    Day
)
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kusto_connection import Kusto_Connection

kc = Kusto_Connection()

class OptiRunner:
    def __init__(self):
        self.how_long = '2d'
        self.bess_characteristics = BessCharacteristics(
            # participating_markets = [m for m in MarketType]
            participating_markets=[MarketType.energy]
        )
        self.bess_state = BessState(
            time_of_measurement = '20250101',
            state_of_energy_mwh = 1.3,
            battery_ac_cycles_since_day_start = 0,
            bess_id = BessId.sovereign_hills,
            battery_inverter_power_mw = 0
        )
        self.cp = [
            CyclingPenalty(upper_limit = 1.5, cost_per_extra_cycle = 20),
            CyclingPenalty(upper_limit = 2.0, cost_per_extra_cycle = 99999999),
            ]
        self.bess = Bess(    
            nameplate_power_capasity = 1000000,
            nameplate_energy_capacity = 2320000,
            max_avail_discharge_power_mw = 1,
            max_avail_charge_power_mw = 0.5,
            soe_floor_mwh = 0.0,
            dispatchable_soe_floor_mwh = 0.38,
            soe_ceil_mwh = 2.235,
            dispatchable_soe_ceil_mwh = 2.1,
            round_trip_efficiency = 0.89,
            parasitic_load_mw = 0.01,
            # registered_markets: list[MarketType]
            cycling_penalties = self.cp
        )

    def get_ts_data(self,when, region):
        whent = dt.datetime.fromisoformat(when)
        q = f"""database('distributionbattery').InfoserverCompositePrices(
        _dispatchPriceTable = database('BtmoInfoserver').DISPATCHPRICE,
        _p5minPriceTable = database('BtmoInfoserver').P5MIN,
        _predispatchPriceTable = database('BtmoInfoserver').PREDISPATCHPRICE,
        _nemRegion = '{region}',
        _windowStartTime = datetime({whent}),
        _windowEndTime = datetime({whent}) + {self.how_long},
        _maxAvailableTime = datetime({whent})
        )"""

        out = kc(db = 'BtmoInfoserver', query=q)

        length = out.shape[0]
        labels = out.source.to_list()
        unix_timestamps = out.startTime.apply(lambda x: int(x.timestamp())).to_list()
        durations = out.duration.apply(lambda x: x.seconds).to_list()
        series = {
            f"{region}.WholesaleSpotPrice":out.wholesaleEnergyRrp.to_list(),
            f"{region}.FcasRaise06SecPrice":out.fcasRaise06SecRrp.to_list(),
            f"{region}.FcasRaise60SecPrice":out.fcasRaise60SecRrp.to_list(),
            f"{region}.FcasRaise05MinPrice":out.fcasRaise05MinRrp.to_list(),
            f"{region}.FcasLower06SecPrice":out.fcasLower06SecRrp.to_list(),
            f"{region}.FcasLower60SecPrice":out.fcasLower60SecRrp.to_list(),
            f"{region}.FcasLower05MinPrice":out.fcasLower05MinRrp.to_list(),
        } 
    
        return TimeseriesData(
            length=length,
            labels=labels,
            unix_timestamps=unix_timestamps,
            durations=durations,
            series=series,
        ), out

# limits=  {
#     Day.monday:[1,0,1],
#     Day.tuesday:[1,0,1],
#     Day.wednesday:[1,0,1],
#     Day.thursday:[1,0,1],
#     Day.friday:[1,0,1],
#     Day.saturday:[1,0,1],
#     Day.sunday:[1,0,1]
#     }

    def do_opti_run(self,d,region):
        when = pd.Timestamp(year=d.year, month=d.month, day=d.day, 
                        hour=0,minute=0,second=0, 
                        tz='Australia/Brisbane')
        when = when.tz_convert('utc')
        when = str(when)
        self.bess_state.time_of_measurement = when
        tsd, k_out = self.get_ts_data(when, region)

        price_forecast = Timeseries(kind = "dp", created = when,metadata = {"NemRegion": region, "RequestTime": str(when) }, data = tsd)

        limits=[1,1,1]
        limits = {x:limits for x in range(7)} if isinstance(limits,list) else limits
        export_lim = NetworkLimit(limit_type=LimitTypes.ExportPowerLimits, times = ["00:00:00", "8:00:00", "17:00:00" ], limits=limits, timezone='Australia/Sydney')

        limits = [1, 1, 1, 1, 1]
        limits = {x:limits for x in range(7)} if isinstance(limits,list) else limits
        import_lim = NetworkLimit(limit_type=LimitTypes.ImportPowerLimits, times = ["00:00:00", "06:50:00", "10:00:00", "14:50:00", "22:00:00" ], limits=limits, timezone='Australia/Sydney')

        limits = [0, 51.247, 0] 
        limits = {x:limits for x in range(7)} if isinstance(limits,list) else limits
        export_reb = NetworkLimit(limit_type=LimitTypes.EXPORTREBATE, times = ["00:00:00", "17:00:00", "20:00:00" ], limits=limits, timezone='Australia/Sydney')
        nw_lims = [export_lim.to_timeseries(price_forecast),import_lim.to_timeseries(price_forecast), 
                export_reb.to_timeseries(price_forecast)
                ]
        
        inp = InputData(
            current_time = when,
            price_forecast = price_forecast,
            bess_characteristics = self.bess_characteristics,
            bess_state = self.bess_state,
            bess = self.bess,
            network_limits = nw_lims
        )

        req = InvocationRequest(
        invocation_id='1234',
        invocation_mode=InvocationMode.FORECAST_TERTIARY,
        data=inp
    )

        tert = "main"
        secondary = 'rad'
        url = 'http://localhost:5063/optimizers?tertiary=PdVer&priceModifiers=DoNothing'
        # url = 'http://localhost:5063/optimizers'
        # url = f"http://localhost:5063/optimizers/s-hilz/invoke?tertiary={tert}&secondary={secondary}&pricemodifiers=DoNothing,CapAllPrices"
        print(url)
        response = requests.post(url, json=req.model_dump())
        print(response)
        out = json.loads(response.content.decode())
        data = out['data']['schedule']['data']

        k_out.startTime = k_out.startTime.apply(lambda x: x.tz_convert(tz='Australia/Brisbane'))

        df = pd.DataFrame(data['series'], index=data['unixTimestamps']).reset_index()
        df = df.rename(columns={'index':'datetime', 'energy':'Power_MW', 'ac_cycle_count_at_interval_end':'cycles', 'soe_at_interval_end':'soe_MWh'})
        df['datetime'] = df.datetime.apply(lambda x: pd.Timestamp.fromtimestamp(x, tz='Australia/Brisbane'))
        df = df.merge(k_out[['startTime', 'wholesaleEnergyRrp']], left_on='datetime', right_on='startTime', how = 'inner' )
        df[['Power_MW','soe_MWh']] = df[['Power_MW','soe_MWh']] /1e6
        df['wholesaleEnergyRrpSc'] = df['wholesaleEnergyRrp'] / df['wholesaleEnergyRrp'].max()
        # df['Power_MW'] = df['Power_MW'] /1e6
        df_1 = df.melt(id_vars='datetime', value_vars=[
            'Power_MW', 
            'soe_MWh', 
            'cycles',
            'wholesaleEnergyRrpSc'
            ])

        df_2 = df.melt(id_vars='datetime', value_vars=[
            'wholesaleEnergyRrp'
            ])

        fig = px.line(df_1, x='datetime',y='value', color='variable')
        return fig
    


#     let t = datatable(timestampUtcCeiling:datetime)
# [ datetime('2025-08-04')];
# let tbl = t
# | make-series max(timestampUtcCeiling) on timestampUtcCeiling from startofday(datetime('2025-08-04')) to endofday(datetime('2025-08-04')) step 5m
# | mv-expand timestampUtcCeiling to typeof(datetime)
# | project time_of_run = timestampUtcCeiling;
# let pd = tbl
# | join kind=leftouter(
# PREDISPATCHPRICE_ADF
# | where LASTCHANGED between (startofday(datetime('2025-08-04')) .. endofday(datetime('2025-08-04')) )
# | where REGIONID == 'NSW1' 
# | project LASTCHANGED,DATETIME,RRP
# | extend interval_end = DATETIME
# | extend time_of_run = bin(LASTCHANGED, 30m)
# | summarize arg_max(LASTCHANGED, *) by interval_end, time_of_run
# | where interval_end > time_of_run
# | project interval_end, time_of_run, RRP
# ) on time_of_run 
# | project-away time_of_run1
# | extend prio = 0, interval_start = interval_end - 30m;
# let p5 = tbl
# | join kind=leftouter (
# P5MIN_REGIONSOLUTION
# | where RUN_DATETIME between (startofday(datetime('2025-08-04')) .. endofday(datetime('2025-08-04')) )
# | where REGIONID == 'NSW1'
# | project RUN_DATETIME, INTERVAL_DATETIME, RRP
# | extend interval_end = INTERVAL_DATETIME
# | extend time_of_run = bin(RUN_DATETIME,5m)
# | summarize arg_max(RUN_DATETIME, *) by interval_end, time_of_run
# | where interval_end > time_of_run
# | project interval_end, time_of_run, RRP
# ) on time_of_run
# | project-away time_of_run1
# | extend prio = 1, interval_start = interval_end - 5m;
# pd
# | union p5
# | where isnotempty(interval_end)
# | summarize arg_max(prio, *) by time_of_run, interval_end
# | sort by time_of_run asc , interval_end asc 