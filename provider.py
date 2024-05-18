import datetime

import polars as pl
from alphata import get_ta_function
from alphaverse.data_provider import BaseDataProvider
from alphaverse.expr.factor import Factor


class PriceProvider(BaseDataProvider):
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_csv("resource/ohlcv.csv").filter(
            pl.col("dt") == dt & pl.col("code") == Factor.name
        )
        return data


class SMAProvider(BaseDataProvider):
    def __init__(self, window: int):
        self.window = window

    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_csv("resource/ohlcv.csv")
        sma = get_ta_function("sma")(data["close"], self.window)
        return sma


class NewPriceProvider(BaseDataProvider):
    def __init__(self, window: int, field: str):
        self._window = window
        self._field = field

    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_csv("resource/ohlcv.csv").sort("dt")
        if self._field == "high":
            df = data.rolling(index_column="dt", period=self._window, group_by="code").agg(
                pl.col(self._field).max().alias(f"{self._field}{self._window}")
            )
        elif self._field == "low":
            df = data.rolling(index_column="dt", period=self._window, group_by="code").agg(
                pl.col(self._field).min().alias(f"{self._field}{self._window}")
            )
        return df


class SmaMomentumProvider(BaseDataProvider):
    def __init__(self, window: int, period: int):
        self._window = window
        self._period = period

    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_csv("resource/ohlcv.csv").sort("dt")
        sma = get_ta_function("sma")(data["close"], self._window)
        df = sma.with_columns()
        return momentum
