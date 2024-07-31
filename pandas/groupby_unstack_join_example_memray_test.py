"""
This can be run from the `pandas` folder as:

    pytest groupby_unstack_join_example_memray_test.py --memray
"""
import pandas as pd
import polars as pl
import numpy as np
from polars_utils import group_by_unstack_polars, join_polars, convert_polars_to_pandas

VERBOSE = False

#######################################################################################################################
# Create a pandas dfframe of calls in a call center. 30M rows and 15 columns
#######################################################################################################################
divisor = 10
size = 30_000_000 // divisor
print(f"Creating a dataframe of {size} rows")
roles = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O"]
df = pd.DataFrame()
df["call_id"] = np.random.randint(0, 10_000, size=size)
df["operator_id"] = np.random.randint(0, 100, size=size)
df["customer_id"] = np.random.randint(0, 100, size=size)
df["operator_role"] = np.random.choice(roles, size=size).astype(str)
df["operator_score_type"] = np.random.choice(["talking", "recording", "teamwork"], size=size).astype(str)

for i in range(1,11):
    df[f"score_{i}"] = np.random.random(size)

df_pl = pl.from_pandas(df)

#######################################################################################################################
# We want to get the sum of score_1 and score_2 for each operator_role and operator_score_type
#######################################################################################################################

def run_pandas_end_to_end(df):
    grouping_cols = ["call_id", "operator_id", "operator_role", "operator_score_type"]
    operator_scores = df.groupby(grouping_cols)[["score_1", "score_2"]].sum()
    # Then for each role, we want to pivot score_1 sums into new columns
    operator_scores = operator_scores["score_1"].unstack("operator_role") 
    operator_scores = operator_scores.rename(columns={role: f"operator{role}_total_score" for role in roles})

    grouping_cols = ["call_id", "customer_id", "operator_role", "operator_score_type"]
    customer_scores = df.groupby(grouping_cols)[["score_1", "score_2"]].sum()
    # Then for each role, we want to pivot score_1 sums into new columns
    customer_scores = customer_scores["score_1"].unstack("operator_role")
    customer_scores = customer_scores.rename(columns={role: f"customer{role}_total_score" for role in roles})

    df_joined = df.join(operator_scores,
                    on=["call_id", "operator_id", "operator_score_type"],
                    lsuffix="_x",
                    rsuffix="_y",
                    how="left",
                    )
    # Join the total customer scores back onto the original df
    df_joined = df_joined.join(customer_scores,
                    on=["call_id", "customer_id", "operator_score_type"], 
                    lsuffix="_x",
                    rsuffix="_y",
                    how="left",
                    )
                

def run_polars_end_to_end(df_pl):
    grouping_cols = ["call_id", "operator_id", "operator_role", "operator_score_type"]
    operator_scores_pl = group_by_unstack_polars(
        df_pl,
        grouping_cols,
        'score_1',
        'operator_role',
    )

    grouping_cols = ["call_id", "customer_id", "operator_role", "operator_score_type"]
    customer_scores_pl = group_by_unstack_polars(
        df_pl,
        grouping_cols,
        'score_1',
        'operator_role',
    )

    df_joined_pl = join_polars(df_pl, operator_scores_pl,
                    on=["call_id", "operator_id", "operator_score_type"],
                    lsuffix="_x",
                    rsuffix="_y",
                    how="left",
                    )
    df_joined_pl = join_polars(df_joined_pl, customer_scores_pl,
                    on=["call_id", "customer_id", "operator_score_type"], 
                    lsuffix="_x",
                    rsuffix="_y",
                    how="left",
                    )

###

# Polars

def test_via_polars_end_to_end():
    for _ in range(10):
        run_polars_end_to_end(df_pl)

def test_pandas_end_to_end():
    for _ in range(10):
        run_pandas_end_to_end(df)
