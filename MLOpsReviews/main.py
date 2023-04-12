from fastapi import FastAPI
import pandas as pd
import numpy as np

platform_dicc = {
    'a': 'Amazon',
    'h': 'Hulu',
    'd': 'Disney Plus',
    'n': 'Netflix'
}
scores_df = pd.read_csv('data/ratings/scores.csv')
sp_dataset = pd.read_csv('data/sp_dataset.csv')

scores_df.rename(columns={'movieId': 'id'}, inplace=True)
temp = scores_df.groupby('id')['score'].agg(['mean'])
big_df = pd.merge(sp_dataset, temp, on='id')


app = FastAPI()


@app.get('/')
def root():
    '''Se da la bienvenida al usuario'''
    txt = 'Bienvenido a mi API.\nEn el buscador ingresa a /docs para visualizar las posibles consultas.'
    return txt


@app.get('/get_max_duration/{year}/{platform}/{duration_type}')
def get_max_duration(year: int | None = None, platform: str | None = None, duration_type: str | None = None):
    '''
    Devuelve la pelicula mas larga
    
    Gets the maximum movie duration from the dataset.'''
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
    # get movie
    temp_df = temp_df.query("type == 'movie'")
    # sort duration
    temp_df = temp_df.sort_values(by='duration_int',
                                  ascending=False)
    return f'{temp_df["title"].iloc[0]}'


@app.get('/get_score_count/{scored}/{year}/{platform}')
def get_score_count(scored: float, year: str, platform: str):
    '''
    Devuelve la cantidad de peliculas de la plataforma y anio,
    que estan valoradas mejor que el score dado

    Gets the movies whose score count is
    over a specified score for a given platform and year'''
    temp_df = big_df
    # get platform
    temp_df = temp_df[temp_df.id.str[0] == platform[0].lower()]
    # get movie
    temp_df = temp_df.query("type == 'movie'")
    # get year
    temp_df = temp_df[temp_df.date_added.str[:4] == str(year)]
    # get score
    temp_df.sort_values(by='mean', ascending=False, inplace=True)
    temp_df = temp_df[temp_df['mean'] > scored]
    qant = temp_df.shape
    return qant[0]


@app.get('/get_count_platform/{platform}')
def get_count_platform(platform: str):
    '''
    Devuelve la cantidad de peliculas de la plataforma dada
    
    Gets the number of movies on the specified platform'''
    # get platform
    temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    # get movie
    temp_df = temp_df.query("type == 'movie'")
    return temp_df["id"].shape[0]


@app.get('/get_actor')
def get_actor(platform: str, year: int):
    '''Returns the actor who appears
    the most in the specified year and platform'''
    temp_df = sp_dataset
    # get platform
    temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    # get year
    temp_df = temp_df[temp_df['release_year'] == year]


@app.get('/prod_per_county/{type}/{country}/{year}')
def prod_per_county(type: str, country: str, year: int):
    '''
    Retorna la cantidad de shows que hay segun el tipo, pais y anio dados
    
    Returns the shows form a given type, country and year'''
    temp_df = sp_dataset
    # get year
    temp_df = temp_df[temp_df.release_year == year]
    # get country
    temp_df = temp_df[temp_df.country == country.lower()]
    # get type
    temp_df = temp_df[temp_df.type == type.lower()]

    return {
        'Pais': country.capitalize(),
        'Anio': year,
        'Peliculas': int(temp_df.shape[0])
    }


@app.get('/get_contents/{rating}')
def get_contents(rating: str):
    '''Retorna la cantidad de shows que tienen la clasificacion dada'''
    temp_df = sp_dataset
    # get rating
    temp_df = temp_df[temp_df.rating == rating.lower()]

    return int(temp_df.shape[0])
