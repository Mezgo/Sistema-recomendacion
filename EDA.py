import ydata_profiling as pdp
import pandas as pd

print("Fetching scores")
scores_df = pd.read_csv(r"data/ratings/scores.csv")
print("Fetching platfomrs")
SP_dataset = pd.read_csv(r"data/sp_dataset.csv")

scores_df.rename(columns={"movieId": "id"}, inplace=True)

temp_df = scores_df.groupby("id")["score"].agg(["mean"])
big_df = pd.merge(SP_dataset, temp_df, on="id")
print(big_df.head())
prof = pdp.ProfileReport(big_df)
prof.to_file(output_file="EDA.html")
