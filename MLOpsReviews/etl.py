from datetime import datetime
import os
import pandas as pd
import numpy as np
import glob


def get_file_names(path: str):
    """
    get the file names from path
    """
    
    files = []
    for file in os.scandir(path):
        if file.is_file():
            if file.name.endswith(".csv"):
                files.append(file.name)
                files.sort()
    return files


def join_csv(path: str, files: list):
    '''
    join the csv files within path into one dataframe
    
    '''
    df = pd.read_csv(path + files[0])
    print(df.shape)
    files.pop(0)

    for e in files:
        print(f"Using file: {e}")
        df2 = pd.read_csv(path + e)
        df = pd.concat([df, df2], ignore_index=True)
        print(df.shape)
    
    return df


def join_df(l: list):
    '''
    joins the dataframes storaged into a list. Uses concat()
    
    '''
    df = l[0]
    print(df.shape)
    l.pop(0)

    for e in l:
        df = pd.concat([df, e], ignore_index=True)
        print(df.shape)
    return df


def get_prefix(l: str):
    '''
    returns the first character of a string
    '''
    return l[0]


def id_generator(df, prefix: str):
    '''
    generates an id conformed by a prefix & the id already in the dataframe
    '''

    print("id_generator")
    for e in range(len(df)):
        df["id"] = prefix + df.iloc[:, 0]
    return df

def get_df(path: str, name: str):
    """This returns a dataframe for each doc in the path
    """
    print(f'Generando DF a partir de {name}')
    df = pd.read_csv(path + name)
    prefix = get_prefix(name)
    print(prefix)
    a = id_generator(df, prefix)
    return a


def convert_2_date(date: str):
    '''
    turns a string into date format (YYYY-mm-dd)
    '''
    
    try:
        months = {
            "january": "01",
            "february": "02",
            "march": "03",
            "april": "04",
            "may": "05",
            "june": "06",
            "july": "07",
            "august": "08",
            "septemeber": "09",
            "october": "10",
            "november": "11",
            "december": "12",
        }
        splt = date.split(" ")
        m = months[splt[0].lower()]
        d = splt[1]
        a = splt[2]
        date = a + "-" + m + "-" + d
        date = datetime.strptime(date, "%Y-%m-%d,")
        return date.date()
    except:
        "null value"
        
def tstamp_2_date(t:str):
    '''turns a timestamp into date'''
    return datetime.fromtimestamp(t).date()

path = r'models/ratings/'
files = get_file_names(path)

df = join_csv(path, files)

print('renombrando columnas')
df.rename(columns={'rating':'score'}, inplace=True)

print('convirtiendo timestamp a date')
for i, e in enumerate(df['timestamp']):
    df['timestamp'][i] = tstamp_2_date(e)

path = r'models/'
streamPlatforms = get_file_names(path)

# en esta lista se almacenaran los df de cada plataforma
platforms = []

# se genera el dataframe con los datos correspondientes a Amazon
Amazon = get_df(path, streamPlatforms[0])
platforms.append(Amazon) # se agrega a la lista anteriormente mencionada

# se genera el dataframe con los datos correspondientes a Disney
Disney_plus = get_df(path, streamPlatforms[1])
platforms.append(Disney_plus)

# se genera el dataframe con los datos correspondientes a Hulu
Hulu = get_df(path, streamPlatforms[2])
platforms.append(Hulu)

# se genera el dataframe con los datos correspondientes a Netfix
Netflix = get_df(path, streamPlatforms[3])
platforms.append(Netflix)

# se unen los datos de las plataformas en un solo dataframe
print('Uniendo los datos de las plataformas')
SP_dataset = join_df([Amazon, Disney_plus, Hulu, Netflix])

print('Reemplazando NaN por G')
SP_dataset['rating'].replace(np.NaN, 'G', inplace=True)

print('Convirtiendo fecha en string a date type')
for i, e in enumerate(SP_dataset['date_added']):
    SP_dataset['date_added'].iloc[i] = convert_2_date(e)

print('convirtiendo duration en int y type')
SP_dataset.insert(loc = 9,
                  column ='duration_int',
                  value = 0)
SP_dataset.insert(loc = 9,
                  column ='duration_type',
                  value = np.NaN)

for i, e in enumerate(SP_dataset['duration']):

    if  type(e) == str:
        e = e.split(' ')
        SP_dataset['duration_type'][i] = e[1]
        SP_dataset['duration_int'][i] = int(int(e[0]))


print('Convirtiendo todos los campos a minusculas')
f = SP_dataset.columns
for e in f:
    try:
        SP_dataset[e] = SP_dataset[e].str.lower()
    except:
        print('null value')

print('Reorganizando SP_dataset')
temp = SP_dataset.pop('id')
SP_dataset.insert(0, temp.name, temp)
del SP_dataset['duration']
del SP_dataset['show_id']

print('creando csv de scores')
df.to_csv('models/ratings/scores.csv', index = False)
print('creando csv de plataformas de streaming')
SP_dataset.to_csv('models/sp_dataset.csv', index = False)
