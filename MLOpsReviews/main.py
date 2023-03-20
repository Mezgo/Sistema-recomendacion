from fastapi import FastAPI
import pandas as pd
from datetime import datetime
import numpy as np

platform_dicc = {
    'a':'Amazon',
    'h':'Hulu',
    'd':'Disney Plus',
    'n':'Netflix'
}

app = FastAPI()

@app.get('/')
def root():
    '''Se da la bienvenida al usuario'''
    return 'Bienvenido a mi API.\nEn el buscador ingresa a /docs para visualizar las posibles consultas.'

@app.get('/get_max_duration')
def get_max_duration(year:int | None = None, platform:str | None = None, duration_type:str | None = None):
    '''Gets the maximum duration of the dataset.'''
    sp_dataset = pd.read_csv('models/sp_dataset.csv')
    temp_df = sp_dataset
    # get platform
    if platform:
        temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    # get year
    if year:
        temp_df = temp_df[temp_df['release_year'] == year]
    # get duration type
    if duration_type:
        temp_df = temp_df[temp_df['duration_type'] == duration_type.lower()]
    # sort duration
    temp_df = temp_df.sort_values(by = 'duration_int',
                                  ascending = False)
    # platform name
    x = platform_dicc.get(temp_df['id'].iloc[0][0])
    return f'The longest show is <{temp_df["title"].iloc[0]}>. It belongs to {x} & was released in {temp_df["release_year"].iloc[0]}. The duration is {(temp_df["duration_int"].iloc[0])} {temp_df["duration_type"].iloc[0]}'

@app.get('/get_score_count')
def  get_score_count(scored:float, year:int, platform:str | None = None):
    scores_df = pd.read_csv('models/ratings/scores.csv')
    sp_dataset = pd.read_csv('models/sp_dataset.csv')
    # get year
    temp_df = scores_df[scores_df['timestamp'].str[:4] == str(year)]
    # get platform
    if platform:
        temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    # get score
    temp_df = scores_df.groupby('movieId')['score'].agg(['mean'])
    temp_df.sort_values(by='mean',ascending=False, inplace=True)
    qant = temp_df[temp_df['mean']>scored].sum()
    # platform name
    x = platform_dicc.get(temp_df.index[0][0])

    return f'On {x}, {str(qant)} is the amount of shows that are upon {scored} on the {year} '

@app.get('/get_count_platform')
def get_count_platform(platform:str):
    sp_dataset = pd.read_csv('models/sp_dataset.csv')

    temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    x = platform_dicc.get(temp_df['id'].iloc[0][0])
    return f'The total amount of shows in {x} is: {temp_df["id"].count()}'

@app.get('/get_actor')
def get_actor(platform:str, year:int):
    '''Returns the actor who appears the most in the specified year and platform'''
    sp_dataset = pd.read_csv('models/sp_dataset.csv')
    temp_df = sp_dataset
    # get platform
    temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    # get year
    temp_df = temp_df[temp_df['release_year'] == year]