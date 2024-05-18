import datetime

import polars as pl
from alphata import get_ta_function
from alphaverse.data_provider import BaseDataProvider
from alphaverse.expr.factor import Factor


class PriceProvider(BaseDataProvider):
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet").filter(
            pl.col("dt") == dt & pl.col("code") == Factor.name
        )
        return data


class SMAProvider(BaseDataProvider):
    def __init__(self, window: int):
        self.window = window

    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet")
        sma = get_ta_function("sma")(data["close"], self.window)
        return sma


class NewPriceProvider(BaseDataProvider):
    def __init__(self, window: int, field: str):
        self._window = window
        self._field = field

    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet").sort("dt")
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
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet").sort("dt")
        sma = get_ta_function("sma")(data["close"], self._window)
        momentum = (
            sma.sort("dt")
            .with_columns(pl.col("code").fill_null(0).pct_change().over("code").alias("return"))
            .with_columns(pl.when(pl.col("return") > 0).then(0).otherwise(-1).alisa("momentum"))
            .rolling(pl.col("momentum"), self._period)
            .agg(pl.col("momentum").sum().alias("continued_momentum"))
        )
        return momentum


class RSProvider(BaseDataProvider):
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        def _calc_rs(data: pl.DataFrame) -> pl.DataFrame:
            df = data.join(index.select(["dt", "return"]), on="dt", suffix="_index").with_columns(
                rs=pl.col("return") / pl.col("return_index")
            )
            return df.select(["dt", "code", "rs"])

        index = (
            pl.read_parquet("resource/index.parquet")
            .sort("dt")
            .with_columns(pl.col("close").pct_change().fill_null(0).over("code").alias("return"))
            .select(["dt", "return"])
        )
        data = (
            pl.read_parquet("resource/kr_stock_ohlcv.parquet")
            .sort("dt")
            .with_columns(pl.col("close").pct_change().fill_null(0).over("code").alias("return"))
            .select(["dt", "code", "return"])
        )
        rs = data.group_by("code").apply(lambda x: _calc_rs(x)).select(["dt", "code", "rs"])
        return rs
