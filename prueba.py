import pandas as pd

scores_df = pd.read_csv("data/ratings/scores.csv")
platform_dicc = {"a": "Amazon", "h": "Hulu", "d": "Disney Plus", "n": "Netflix"}
# temp = scores_df
# scored = 4.0
# year = 2015
# platform = 'disney'
# # get year
# temp_df = scores_df[scores_df['timestamp'].str[:4] == str(year)]
# # get platform
# temp_df = temp_df[temp_df['movieId'].str[0] == platform[0].lower()]
# # get score
# temp_df = temp_df.groupby('movieId')['score'].agg(['mean'])
# temp_df.sort_values(by='mean',ascending=False, inplace=True)
# temp_df = temp_df[temp_df['mean']>scored]
# qant = temp_df.shape
# # platform name
# x = platform_dicc.get(temp_df.index[0][0])

# print(f'On {x}, {qant[0]} is the amount of shows
# that are upon {scored} on the {year}')

######################################################
platform = "hulu"
sp_dataset = pd.read_csv("data/sp_dataset.csv")
"""Gets the number of shows on the specified platform"""
temp_df = sp_dataset[sp_dataset["id"].str[0] == platform[0].lower()]
temp_df = sp_dataset.query("type == 'movie'")
p_name = platform_dicc.get(temp_df["id"].iloc[0][0])
print(f'The total amount of shows in {p_name} is: {temp_df["id"].count()}')
