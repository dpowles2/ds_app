from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
import datetime as dt
import pandas as pd
from enum import Enum, IntEnum



class BaseJSONModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        serialize_by_alias=True,
        populate_by_name=True,
        from_attributes=True,
        # frozen=True,
        use_enum_values=True,
    )

class TimeseriesData(BaseJSONModel):
    length: int
    labels: list[str] | None = None
    unix_timestamps: list[int]
    durations: list[int] | None = None
    series: dict[str, list[float | None]]

class Timeseries(BaseJSONModel):
    kind: str
    created: str
    metadata: dict[str, str]
    data: TimeseriesData

class MarketType(str, Enum):  # Types of markets available
    energy = (
        "wholesale_spot"  # TODO we have a naming clash Energy v. wholesale spot FIXIT
    )
    raise_01_sec = "fcas_raise_01_sec"
    raise_06_sec = "fcas_raise_06_sec"
    raise_60_sec = "fcas_raise_60_sec"
    raise_05_min = "fcas_raise_05_min"
    lower_01_sec = "fcas_lower_01_sec"
    lower_06_sec = "fcas_lower_06_sec"
    lower_60_sec = "fcas_lower_60_sec"
    lower_05_min = "fcas_lower_05_min"

class BessCharacteristics(BaseJSONModel):
    # these values are transmitted in base units, (watts, watt-hours, etc.)
    participating_markets: list[MarketType] | None = None

class BessId(Enum):
    sovereign_hills = "azzo_sovereign_hills_1"
    agl_lab_battery = "tesla_lab_group_1"
    sa_sheds_1 = "tesla_sa_gov_group_1"
    sa_sheds_2 = "tesla_sa_gov_group_2"
    brockman = "brockman_1"

class BessState(BaseJSONModel):
    time_of_measurement: str
    state_of_energy_mwh: float
    battery_ac_cycles_since_day_start: float
    bess_id: BessId | str | None = None
    battery_inverter_power_mw: float | None = None

class CyclingPenalty(BaseJSONModel):
    upper_limit: float
    cost_per_extra_cycle: float

class Bess(BaseJSONModel):
    # nem_region:
    nameplate_power_capasity: float | None = None
    nameplate_energy_capacity: float
    max_avail_discharge_power_mw: float
    max_avail_charge_power_mw: float
    soe_floor_mwh: float
    dispatchable_soe_floor_mwh: float
    soe_ceil_mwh: float
    dispatchable_soe_ceil_mwh: float
    round_trip_efficiency: float
    parasitic_load_mw: float
    # registered_markets: list[MarketType]
    cycling_penalties: list[CyclingPenalty]

class LimitTypes(Enum):
    ExportPowerLimits = 'ExportPowerLimitsMw'
    ImportPowerLimits = 'ImportPowerLimitsMw'
    EXPORTREBATE = "ExportRebateDolPerMWh"

class Day(IntEnum):
    monday = 0
    tuesday = 1
    wednesday = 2
    thursday = 3
    friday = 4
    saturday = 5
    sunday = 6

class NetworkLimit(BaseJSONModel):
    limit_type: LimitTypes | str
    times: list[str]
    limits: dict[int | Day, list[float]]
    timezone: str

    def ungroup(self, df: pd.DataFrame, timestep_minutes: int = 300) -> pd.DataFrame:
        timestep = timestep_minutes
        # find number of groups each row is to be ungrouped into
        df["intervals"] = df.durations.apply(
            lambda x: x // timestep if x >= timestep else 1
        )
        
        # break row into one row per interval
        df["unix_timestamps"] = df.apply(
            lambda x: [x["unix_timestamps"] + j * timestep for j in range(x["intervals"])],
            axis=1,
        )
        df = (
            df.explode(column="unix_timestamps")
            # .drop(columns=["index", "intervals"])
            .reset_index(drop=True)
        )

        # convert durations column to equal timestep
        df["durations"] = df.durations.apply(lambda x: x if x < timestep else timestep)

        return df.drop(columns='intervals')

    def get_limit(self, day, time):
        index = 0
        for i,t in enumerate(self.times):
            if str(time) >= t:
                continue
            index = i-1
            break

        return self.limits[day][index]

    def to_timeseries(self, ts: Timeseries) -> Timeseries:
        kind = self.limit_type
        created = str(dt.datetime.now())
        metadata = {'hello':'hi'}
        # convert unix 
        unix_timestamps = ts.data.unix_timestamps
        durations = ts.data.durations
        df = pd.DataFrame({'unix_timestamps': unix_timestamps,'durations':durations})
        df = self.ungroup(df)
        df['local_timestamps'] = df.unix_timestamps.apply(lambda x: pd.Timestamp.fromtimestamp(x, tz='Australia/Sydney'))
        
        df['day'] = df.local_timestamps.apply(lambda x: x.day_of_week)
        df['time'] = df.local_timestamps.apply(lambda x: str(x.time()))
        time_df = pd.DataFrame(self.limits, index=self.times).reset_index(names='time').melt(id_vars = 'time', var_name = 'day',value_name= 'limit')
        
        df = df.merge(time_df, on=['day','time'], how='left').sort_values('unix_timestamps')

        df.loc[0,'limit'] = self.get_limit(df.loc[0,'day'],df.loc[0,'time']) if pd.isna(df.loc[0,'limit']) else df.loc[0,'limit']
        
        cur_lim = df.loc[0,'limit']
        period = 0
        df['period'] = period
        for i in range(df.shape[0]):
            if pd.isna(df.loc[i,'limit']):
                df.loc[i,'limit'] = df.loc[i-1,'limit']
            if df.loc[i,'limit'] != cur_lim:
                period += 1
                cur_lim = df.loc[i,'limit']
            df.loc[i, 'period'] = period

        df = df.groupby('period').agg({"unix_timestamps":'min', 'durations':'sum','limit':'max'})

        
        unix_times = df.unix_timestamps
        durations = df.durations
        return Timeseries(
            kind = kind, created = created, metadata=metadata,
            data = TimeseriesData(
                length = df.shape[0], 
                unix_timestamps=unix_times,
                durations=durations,
                series = {"PowerLimits": df.limit}
            )
        ) 
    
class InputData(BaseJSONModel):
    current_time: str
    price_forecast: Timeseries
    bess_characteristics: BessCharacteristics
    bess_state: BessState
    bess: Bess | None = None
    network_limits: list[Timeseries] | None = None
    # dispatch_plan: Timeseries | None = None

class InvocationMode(Enum):
    FORECAST_SECONDARY = "ForecastSecondary"
    FORECAST_TERTIARY = "ForecastTertiary"
    BACKCAST = "Backcast"

class InvocationRequest(BaseJSONModel):
    invocation_id: str
    invocation_mode: InvocationMode
    data: InputData

        
        


