import pandas as pd
import numpy as np
import time


#######################################################################################################################
# Create a pandas dataframe of 30M rows and ~40 columns for the left side of a merge_asof
#######################################################################################################################
left_df_size = 30_000_000
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
right_df_size = 5_000_000
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



#######################################################################################################################
# Now perform the merge_asof
#######################################################################################################################
start = time.time()
merge_columns = ["join_float", "join_float2"]
df= pd.merge_asof(
    left_df.sort_values(by="merge_time"),
    right_df.sort_values(by="merge_time"),
    left_on="merge_time",
    right_on="merge_time",
    by=merge_columns,
    tolerance=pd.Timedelta("30m"),
)
print(f"Time taken to perform merge_asof: {time.time() - start:.2f} seconds")