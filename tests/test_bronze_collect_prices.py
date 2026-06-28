import pandas as pd

from pipelines.bronze.collect_prices import filter_incremental_rows


def test_filter_incremental_rows_keeps_only_dates_after_last_stored_date() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-06-26", "2026-06-27", "2026-06-28"]),
            "asset": ["BBAS3", "BBAS3", "BBAS3"],
        }
    )

    result = filter_incremental_rows(df, pd.Timestamp("2026-06-27"))

    assert result["date"].tolist() == [pd.Timestamp("2026-06-28")]


def test_filter_incremental_rows_without_last_stored_date_returns_input() -> None:
    df = pd.DataFrame({"date": pd.to_datetime(["2026-06-26"])})

    result = filter_incremental_rows(df, None)

    assert result.equals(df)
