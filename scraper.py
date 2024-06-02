import datetime
from typing import Any

import click
import polars as pl
import requests


@click.command()
@click.option(
    "--dt",
    "-d",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.datetime.today().strftime("%Y-%m-%d"),
    help="Date to update data",
)
def main(dt: datetime.datetime):
    if _is_holiday(dt):
        return
    for type in ["stock", "index"]:
        update_data(type, dt)


def update_data(type: str, dt: datetime.datetime):
    if type == "stock":
        path = "resource/kr_stock_ohlcv.parquet"
        data = fetch_stock_ohlcv(dt)
    else:
        path = "resource/index.parquet"
        data = fetch_index_ohlcv(dt)
    df = pl.read_parquet(path)
    df = pl.concat([df, data], how="vertical_relaxed").unique(subset=["dt", "code"], keep="last")
    df.write_parquet(path)


def _is_holiday(dt: datetime.datetime) -> bool:
    if dt.weekday() >= 5:
        return True
    holdidays = get_krx_holiday(dt)
    if dt.date() in holdidays:
        return True
    return False


def get_krx_holiday(dt: datetime.datetime) -> list[datetime.date]:
    url = "https://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx"
    pagepath = "contents/MKD/01/0110/01100305/MKD01100305"
    body = {
        "search_bas_yy": str(dt.year),
        "gridTp": "KRX",
        "pagePath": f"{pagepath}.jsp",
    }
    body["code"] = _get_otp_data(pagepath)
    headers = {"Referer": f"http://open.krx.co.kr/{pagepath}.jsp"}
    res = requests.post(url=url, data=body, headers=headers)
    data = res.json()
    return [
        datetime.datetime.strptime(each["calnd_dd_dy"], "%Y-%m-%d").date()
        for each in data["block1"]
    ]


def _get_otp_data(pagepath: str) -> str:
    headers = {
        "Referer": f"https://open.krx.co.kr/{pagepath}.jsp",
        "User-Agent": "UserAgent",
    }
    gen_otp_data = {
        "bld": "MKD/01/0110/01100305/mkd01100305_01",
        "name": "form",
        "_": str(int(float(datetime.datetime.now().strftime("%s.%f")) * 1e3)),
    }
    res = requests.get(
        url="http://open.krx.co.kr/contents/COM/GenerateOTP.jspx",
        params=gen_otp_data,
        headers=headers,
    )
    return res.text


def fetch_stock_ohlcv(dt: datetime.datetime) -> pl.DataFrame:
    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "locale": "ko_KR",
        "mktId": "ALL",
        "trdDd": dt.strftime("%Y%m%d"),
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }
    data = _api_reqeust(payload)
    df = pl.DataFrame(data["OutBlock_1"])
    return (
        df.rename({
            "ISU_SRT_CD": "code",
            "MKT_NM": "market",
            "TDD_CLSPRC": "close",
            "TDD_OPNPRC": "open",
            "TDD_HGPRC": "high",
            "TDD_LWPRC": "low",
            "ACC_TRDVOL": "volume",
            "ACC_TRDVAL": "volume_valued",
        })
        .with_columns(dt=pl.lit(dt))
        .select(
            pl.col("dt").cast(pl.Datetime),
            pl.col("code"),
            pl.col("open").str.replace_all(",", "").cast(pl.Float64),
            pl.col("high").str.replace_all(",", "").cast(pl.Float64),
            pl.col("low").str.replace_all(",", "").cast(pl.Float64),
            pl.col("close").str.replace_all(",", "").cast(pl.Float64),
            pl.col("volume").str.replace_all(",", "").cast(pl.Float64),
            pl.col("volume_valued").str.replace_all(",", "").cast(pl.Float64),
        )
    )


def fetch_index_ohlcv(dt: datetime.datetime) -> pl.DataFrame:
    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT00101",
        "locale": "ko_KR",
        "idxIndMidclssCd": "02",  # 02: KOSPI, 03: KOSDAQ
        "trdDd": dt.strftime("%Y%m%d"),
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }
    data = _api_reqeust(payload)
    df = pl.DataFrame(data["output"])
    return (
        df.rename({
            "CLSPRC_IDX": "close",
            "OPNPRC_IDX": "open",
            "HGPRC_IDX": "high",
            "LWPRC_IDX": "low",
            "ACC_TRDVOL": "volume",
            "ACC_TRDVAL": "volume_valued",
        })
        .filter(pl.col("IDX_NM") == "코스피")
        .with_columns(dt=pl.lit(dt), code=pl.lit("kospi"))
        .select(
            pl.col("dt").cast(pl.Datetime),
            pl.col("code"),
            pl.col("open").str.replace_all(",", "").cast(pl.Float64),
            pl.col("high").str.replace_all(",", "").cast(pl.Float64),
            pl.col("low").str.replace_all(",", "").cast(pl.Float64),
            pl.col("close").str.replace_all(",", "").cast(pl.Float64),
            pl.col("volume").str.replace_all(",", "").cast(pl.Float64),
            pl.col("volume_valued").str.replace_all(",", "").cast(pl.Float64),
        )
    )


def _api_reqeust(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201",
    }
    res = requests.post(url, data=payload, headers=headers)
    if res.status_code != 200:
        raise Exception("Failed to fetch data")
    data = res.json()
    return data


if __name__ == "__main__":
    main()
