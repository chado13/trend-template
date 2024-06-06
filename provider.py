import datetime

import polars as pl
from alphata import get_ta_function
from alphaverse.data_provider import BaseDataProvider
from alphaverse.expr.factor import Factor


class PriceProvider(BaseDataProvider):
    def __init__(self, field: str):
        self._field = field

    @property
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet").filter(
            pl.col("dt") == pl.col("dt").max()
        )
        df = data.select(["code", self._field])
        return df


class SMAProvider(BaseDataProvider):
    def __init__(self, window: int):
        self.window = window

    @property
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet")
        func = get_ta_function("sma")
        sma = data.group_by("code").map_groups(
            lambda df: df.with_columns(func(df["close"], self.window).alias(f"sma{self.window}"))
        )
        return sma.filter(pl.col("dt") == pl.col("dt").max()).select("code", f"sma{self.window}")


class NewPriceProvider(BaseDataProvider):
    def __init__(self, window: int, field: str):
        self._window = window
        self._field = field

    @property
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet").sort("dt")
        if self._field == "high":
            df = data.rolling(index_column="dt", period=f"{self._window}w", by="code").agg(
                pl.col(self._field).max().alias(f"{self._field}{self._window}")
            )
        elif self._field == "low":
            df = data.rolling(index_column="dt", period=f"{self._window}w", by="code").agg(
                pl.col(self._field).min().alias(f"{self._field}{self._window}")
            )
        return df.filter(pl.col("dt") == pl.col("dt").max()).select(
            "code", f"{self._field}{self._window}"
        )


class SmaMomentumProvider(BaseDataProvider):
    def __init__(self, window: int, period: int):
        self._window = window
        self._period = period

    @property
    def symbol_column_name(self) -> str:
        return "code"

    async def fetch(self, factor: Factor, dt: datetime.datetime) -> pl.DataFrame:
        data = pl.read_parquet("resource/kr_stock_ohlcv.parquet").sort("dt")
        func = get_ta_function("sma")
        sma = data.group_by("code").map_groups(
            lambda df: df.with_columns(func(df["close"], self._window).alias(f"sma{self._window}"))
        )
        momentum = sma.sort("dt").with_columns(
            pl.col("close").diff().shift(self._period).over("code").alias("sma_momentum")
        )
        return momentum.filter(pl.col("dt") == pl.col("dt").max()).select("code", "sma_momentum")


class RSProvider(BaseDataProvider):
    @property
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
        rs = data.group_by("code").apply(lambda x: _calc_rs(x))
        return rs.filter(pl.col("dt") == pl.col("dt").max()).select(["code", "rs"])
