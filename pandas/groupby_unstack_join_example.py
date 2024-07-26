import pandas as pd
import numpy as np
import time

#######################################################################################################################
# Create a pandas dfframe of calls in a call center. 30M rows and 15 columns
#######################################################################################################################
size = 30_000_000
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

#######################################################################################################################
# We want to get the sum of score_1 and score_2 for each operator_role and operator_score_type
#######################################################################################################################
unstack_start = time.time()
grouping_cols = ["call_id", "operator_id", "operator_role", "operator_score_type"]
operator_scores = df.groupby(grouping_cols)[["score_1", "score_2"]].sum()
# Then for each role, we want to pivot score_1 sums into new columns
operator_scores = operator_scores["score_1"].unstack("operator_role") 
operator_scores = operator_scores.rename(columns={role: f"operator{role}_total_score" for role in roles})
print(f"Operator Unstacking took {time.time() - unstack_start:.2f} seconds")

#######################################################################################################################
# Now do the same but group on customer_id rather than operator_id to get scores for each call/customer/operator_role/operator_score_type
#######################################################################################################################

unstack_start = time.time()
grouping_cols = ["call_id", "operator_id", "operator_role", "operator_score_type"]
grouping_cols = ["call_id", "customer_id", "operator_role", "operator_score_type"]
customer_scores = df.groupby(grouping_cols)[["score_1", "score_2"]].sum()
# Then for each role, we want to pivot score_1 sums into new columns
customer_scores = customer_scores["score_1"].unstack("operator_role")
customer_scores = customer_scores.rename(columns={role: f"customer{role}_total_score" for role in roles})
print(f"Customer Unstacking took {time.time() - unstack_start:.2f} seconds")


#######################################################################################################################
# Join the total operator scores back onto the original df
#######################################################################################################################
join_start = time.time()
df = df.join(operator_scores,
                 on=["call_id", "operator_id", "operator_score_type"],
                 lsuffix="_x",
                 rsuffix="_y",
                 how="left",
                 )
#######################################################################################################################
# Join the total customer scores back onto the original df
#######################################################################################################################
df = df.join(customer_scores,
                 on=["call_id", "customer_id", "operator_score_type"], 
                 lsuffix="_x",
                 rsuffix="_y",
                 how="left",
                 )
print(f"both joins took {time.time() - join_start:.2f} seconds")
