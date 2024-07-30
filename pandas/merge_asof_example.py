import pandas as pd
from datetime import timedelta
import polars as pl
import numpy as np
import time


#######################################################################################################################
# Create a pandas dataframe of 30M rows and ~40 columns for the left side of a merge_asof
#######################################################################################################################
divisor = 10

left_df_size = 30_000_000 // divisor
left_df = pd.DataFrame()
# the join will have 2 float columns as the keys to join on before performing the merge_asof (these are used in the 'by' parameter)
left_df["join_float"] = np.random.choice(np.linspace(-10, 10, 81), size=left_df_size).astype(np.float64) # handicap
left_df["join_float2"] = np.random.choice(np.linspace(0, 40, 161), size=left_df_size).astype(np.float64) # selection_id_mapped
# add a time col for the merge_asof
left_df["merge_time"] = np.random.choice(pd.date_range("2023-09-01", "2023-09-02", freq="ms"), size=left_df_size) # bet_action_time

# Now that we have the join columns done we will add an assortment of floats, integers, strings and timestamps
# add some ints
left_df["int_1"] = np.random.randint(low=0, high=5_000_000, size=left_df_size).astype(np.int64) # selection_id_hub                           
left_df["int_2"] = np.random.randint(low=0, high=5_000_000, size=left_df_size).astype(np.int64) # selection_id_hub_inverse                           
left_df["int_3"] = np.random.randint(low=0, high=5_000_000, size=left_df_size).astype(np.int64) # selection_id_hub                           
left_df["int_4"] = np.random.randint(low=0, high=5_000_000, size=left_df_size).astype(np.int64) # selection_id_hub_inverse                           
# add 25 float_64s
for i in range(1, 26):
    left_df[f"float64_{i}"] = np.random.random(size=left_df_size).astype(np.float64) # odds

# Add a couple of timestamps
left_df["time1"] = np.random.choice(pd.date_range("2023-09-01", "2023-09-02", freq="ms"), size=left_df_size) # bet_action_time
left_df["time2"] = np.random.choice(pd.date_range("2023-09-01", "2023-09-02", freq="ms"), size=left_df_size) # bet_action_time

# add 10 string cols 
for i in range(1,11):
    left_df[f"string_{i}"] = np.random.randint(low=0, high=5_000_000, size=left_df_size).astype(str)


#######################################################################################################################
# Create a pandas dataframe of 5M rows and 8 columns for the right side of a merge_asof
#######################################################################################################################
right_df_size = 5_000_000 // divisor
right_df = pd.DataFrame()
# the join will have 2 float columns as the keys to join on before performing the merge_asof (these are used in the 'by' parameter)
right_df["join_float"] = np.random.choice(np.linspace(-10, 10, 81), size=right_df_size).astype(np.float64) # handicap
right_df["join_float2"] = np.random.choice(np.linspace(0, 40, 161), size=right_df_size).astype(np.float64) # selection_id_mapped
# add a time col for the merge_asof
right_df["merge_time"] = np.random.choice(pd.date_range("2023-09-01", "2023-09-02", freq="ms"), size=right_df_size) # bet_action_time
# add another float column
right_df["float1"] = np.random.random(size=right_df_size).astype(np.float64) # match_id
# add string cols 
right_df["date"] = "2023-09-01" # match_start_date
right_df["string1"] = np.random.choice(["aaa", "bbbb", "ccccc", "dddddd", "eeeeeee"], size=right_df_size) # action
right_df["string2"] = np.random.randint(low=0, high=5_000_000, size=right_df_size).astype(str) # bet_action_id
right_df["string3"] = np.random.choice(["aaa", "bbbb", "ccccc", "dddddd", "eeeeeee"], size=right_df_size) # source



def merge_asof_via_polars(
    left,
    right,
    on=None,
    left_on=None,
    right_on=None,
    by=None,
    tolerance=None,
):
    # Take pandas input, converts to Polars, apply `join_asof`, convert back to pandas.
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
    cols = {}
    for col in result.columns:
        if result[col].dtype == pl.String():
            cols[col] = result[col].to_pandas(use_pyarrow_extension_array=True)
        else:
            cols[col] = result[col].to_pandas()
    return pd.DataFrame(cols)

def merge_asof_via_polars_no_conversion(
    left,
    right,
    on=None,
    left_on=None,
    right_on=None,
    by=None,
    tolerance=None,
):
    # Like the above, but assumes input is already Polars
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

    left_pl = left.sort(left_on)
    right_pl = right.sort(right_on)
    result = left_pl.join_asof(
        right_pl,
        on=on,
        left_on=left_on,
        right_on=right_on,
        by=by,
        tolerance=tolerance,
    )
    return result

#######################################################################################################################
# Now perform the merge_asof
#######################################################################################################################
start = time.perf_counter()
merge_columns = ["join_float", "join_float2"]
df= pd.merge_asof(
    left_df.sort_values(by="merge_time"),
    right_df.sort_values(by="merge_time"),
    left_on="merge_time",
    right_on="merge_time",
    by=merge_columns,
    tolerance=pd.Timedelta("30m"),
)
print(f"Time taken to perform merge_asof: {time.perf_counter() - start:.2f} seconds")
print(df)


start = time.perf_counter()
merge_columns = ["join_float", "join_float2"]
df_fast= merge_asof_via_polars(
    left_df,
    right_df,
    left_on="merge_time",
    right_on="merge_time",
    by=merge_columns,
    tolerance=timedelta(minutes=30),
)
print(f"Time taken to perform merge_asof via Polars: {time.perf_counter() - start:.2f} seconds")
print(df_fast)

# check results actually match
if divisor >= 1000:
    df[df.dtypes[df.dtypes==object].index] = df[df.dtypes[df.dtypes==object].index].astype('large_string[pyarrow]')
    pd.testing.assert_frame_equal(
        df.sort_values(merge_columns).reset_index(drop=True),
        df_fast.sort_values(merge_columns).reset_index(drop=True),
    )

left_pl = pl.from_pandas(left_df)
right_pl = pl.from_pandas(right_df)
start = time.perf_counter()
merge_columns = ["join_float", "join_float2"]
df_fast= merge_asof_via_polars_no_conversion(
    left_pl,
    right_pl,
    left_on="merge_time",
    right_on="merge_time",
    by=merge_columns,
    tolerance=timedelta(minutes=30),
)
print(f"Time taken to perform merge_asof via Polars (not including data conversion): {time.perf_counter() - start:.2f} seconds")
print(df_fast)
