import polars as pl
import pandas as pd


def merge_asof_polars(
    left,
    right,
    on=None,
    left_on=None,
    right_on=None,
    by=None,
    tolerance=None,
):
    if on is not None:
        if left_on is not None:
            raise ValueError("Can only pass one of 'on' and 'left_on'")
        if right_on is not None:
            raise ValueError("Can only pass one of 'on' and 'right_on'")
        left_on = on
        right_on = on
    else:
        if left_on is None or right_on is None:
            raise ValueError("Must pass 'on' or 'left_on' and 'right_on'")

    left_pl = pl.from_pandas(left).sort(left_on)
    right_pl = pl.from_pandas(right).sort(right_on)
    result = left_pl.join_asof(
        right_pl,
        on=on,
        left_on=left_on,
        right_on=right_on,
        by=by,
        tolerance=tolerance,
    )
    return result

def convert_polars_to_pandas(df):
    """
    Convert Polars dataframe to pandas.

    String columns get converted to pandas with the `use_pyarrow_extension_array=True` option,
    as pyarrow strings offer significant advantages in pandas. Other columns just get
    converted to the respective default type in pandas, as pyarrow dtypes aren't mature enough
    in pandas 1.5 (the maximum pandas version for Python 3.8).
    """
    cols = {}
    for col in df.columns:
        if df[col].dtype == pl.String():
            cols[col] = df[col].to_pandas(use_pyarrow_extension_array=True)
        else:
            cols[col] = df[col].to_pandas()
    return pd.DataFrame(cols)

def group_by_unstack_polars(
    df_pl,
    grouping_cols,
    unstack_value_col,
    unstack_column,
):
    pivoted = df_pl.pivot(
        on=unstack_column,
        index=[x for x in grouping_cols if x!=unstack_column],
        values=unstack_value_col,
        aggregate_function='sum',
    )
    pivoted = pivoted.select(sorted(pivoted.columns)).rename({x: f"{unstack_column}_{x}_{unstack_value_col}" for x in pivoted.columns if x not in grouping_cols})
    return pivoted.sort([x for x in grouping_cols if x != unstack_column])

def join_polars(
    left_pl,
    right_pl,
    on,
    how,
    lsuffix,
    rsuffix,
):
    left_cols = left_pl.columns
    right_cols = right_pl.columns
    joined = left_pl.join(
        right_pl,
        on=on,
        suffix=rsuffix,
        how=how,
        coalesce=False,
    )
    joined = joined.rename({x: f"{x}{lsuffix}" for x in set(left_cols).intersection(right_cols).difference(on)})
    joined = joined.drop([f"{x}{rsuffix}" for x in on])
    return joined
