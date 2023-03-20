import pandas as pd
from datetime import datetime
import numpy as np

scores_df = pd.read_csv('models/ratings/scores.csv')
platform_dicc = {
    'a':'Amazon',
    'h':'Hulu',
    'd':'Disney Plus',
    'n':'Netflix'
}
temp = scores_df
scored = 4.0
year = 2015
platform = 'disney'
# get year
temp_df = scores_df[scores_df['timestamp'].str[:4] == str(year)]
# get platform
temp_df = temp_df[temp_df['movieId'].str[0] == platform[0].lower()]
# get score
temp_df = temp_df.groupby('movieId')['score'].agg(['mean'])
temp_df.sort_values(by='mean',ascending=False, inplace=True)
temp_df = temp_df[temp_df['mean']>scored]
qant = temp_df.shape
# platform name
x = platform_dicc.get(temp_df.index[0][0])

print(f'On {x}, {qant[0]} is the amount of shows that are upon {scored} on the {year}')