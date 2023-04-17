
# Sistema-recomendacion

## ETL (Definicion de funciones)

A continuacion se definen las funciones que permiten la creacion y manipulacion de los conjuntos de datos utilizados en la primera etapa del proyecto.

Esta funcion permite crear una lista con los nombres de los archivos `csv` presentes en un directorio.

 ```python
 def get_file_names(path:str):
    files = []
    for file in os.scandir(path):
        if file.is_file():
            if file.name.endswith('.csv'):
                files.append(file.name)
                files.sort()
    return files
```

Esta funcion permite crear y unir dataframes a partir de sus archivos originarios `csv`. Recibe la ruta en la que se encuentran los archivos a unir y una lista con sus nombres.

```python
def join_csv(path: str, files: list):
    df = pd.read_csv(path+files[0])
    print(df.shape)
    files.pop(0)

    for e in files:
        print(f'Using file: {e}')
        df2 = pd.read_csv(path+e)
        df = pd.concat([df,df2])
        print(df.shape)
    print(df.shape)
    return df
```

La siguiente funcion recibe una lista de dataframes para unirlos en uno solo.

```python
def join_df(l:list):
    df = l[0]
    print(df.shape)
    l.pop(0)
    for e in l:
        df = pd.concat([df,e])
        print(df.shape)
    print(df.shape)
    return df
```

La funcion a continuacion permite extraer la primera letra de un _string_ dado.

```python
def get_prefix(l:str):
    return l[0]
```

Con la siguiente funcion se crea el `id` que combina los valores del campo `show_Id` y el resutado de la funcion descrita anteriormente.

```python
def id_generator(df, prefix:str):
   print('idGenerator')
   for e in range(len(df)):
       df['id'] = prefix+df.iloc[:, 0]
   return df
```

La siguiente funcion levanta un dataframe a partir de un `path` entregado y el nombre del archivo `csv`.

```python
def get_df(path:str, name:str):
    df = pd.read_csv(path+name)
    prefix = getPrefix(name)
    print(prefix)
    a = idGenerator(df, prefix)
    return a
```

Para obtener una fecha en formato (YYYY-mm-dd) la fecha a partir de una cadena de caracteres.

```python
def convert_2_date(date: str):
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
```

La siguiente funcion permite la transformacion del formato `datetime` a `date`.

```python
def tstamp_2_date(t:str):
    return datetime.fromtimestamp(t).date()
```

> Para infomacion detallada acerca del flujo de trabajo en esta estapa del proyecto, revisar el archivo `ETL.ipynb`.
---

## Desarrollo API (consultas)

Para el desarrollo de la API se ha usado FastAPI.
Su desarrollo y caracteristicas seran explicados acontinuacion.

__Se ha creado un diccionario el cual permitira mapear las primeras letras de los nombres de cada plataforma de _streaming_ con su nombre completo.__

```python
platform_dicc = {
    'a':'Amazon',
    'h':'Hulu',
    'd':'Disney Plus',
    'n':'Netflix'
}
```

Se recogen los archivos generados en el ___ETL___.

```python
scores_df = pd.read_csv('data/ratings/scores.csv')
sp_dataset = pd.read_csv('data/sp_dataset.csv')
```

En la ruta raiz del sitio se da la bienvenida.

```python
def root():
    '''Se da la bienvenida al usuario'''
    return 'Bienvenido a mi API.\nEn el buscador ingresa a /docs para visualizar las posibles consultas.'
```

__Se genera la primera ruta, la cual contiene a la consulta `get_max_duration`.__

```python
def get_max_duration(year:int | None = None, platform:str | None = None, duration_type:str | None = None):
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
```

A lo largo de esta definicion se realiza lo siguiente:

0. Se definen las variables que van a ser introducidas a la consulta. Se establece su tipo y valor por defecto en caso de ser omitidas.

1. Se realiza una copia del dataframe original que e usara en esta consulta.

2. Si el valor `platfom` es introducido, se realiza el correspondiente filtrado.

3. Si el valor `year` es introducido, se realiza el correspondiente filtrado.

4. Si el valor `duration_type` es introducido, se realiza el correspondiente filtrado.

5. Se ordena el dataframe obtenido, por `duration_int` en orden descendente.

6. Se obtiene el nombre de la plataforma a travez del diccionario creado anteriormente.

__Se genera la segunda ruta, la cual contiene a la consulta `get_score_count`.__

```python
def get_score_count(scored:float, year:str, platform:str):
    temp_df = scores_df
    # get year
    temp_df = temp_df[temp_df['timestamp'].str[:4] == year]
    # get platform
    temp_df = temp_df[temp_df['movieId'].str[0] == platform[0].lower()]
    # get score
    temp_df = temp_df.groupby('movieId')['score'].agg(['mean'])
    temp_df.sort_values(by='mean',ascending=False, inplace=True)
    temp_df = temp_df[temp_df['mean']>scored]
    qant = temp_df.shape
    # platform name
    x = platform_dicc.get(temp_df.index[0][0])
    return f'On {x}, {qant[0]} is the amount of shows that are upon {scored} on the {year}'
```

A lo largo de esta definicion se realiza lo siguiente:

0. Se definen las variables que van a ser introducidas a la consulta.

1. Se realiza una copia del dataframe original que e usara en esta consulta.

2. Para el valor `year`, se realiza el correspondiente filtrado.

3. Para el valor `platfom`, se realiza el correspondiente filtrado.

4. Para filtrar por el `score` introducido, se realiza un `gorupby` en el dataframe que contiene los _ratings_ de las peliculas; se agrupa por `score` y se aplica `mean` como funcion de agregacion.

5. Se captura la cantidad de datos resultantes en el dataframe.

6. Se obtiene el nombre de la plataforma a travez del diccionario creado anteriormente.

__Se genera la tercera ruta, la cual contiene a la consulta `get_count_platform`.__

```python
def get_count_platform(platform:str):
    temp_df = sp_dataset[sp_dataset['id'].str[0] == platform[0].lower()]
    x = platform_dicc.get(temp_df['id'].iloc[0][0])
    return f'The total amount of shows in {x} is: {temp_df["id"].count()}'
```

A lo largo de esta definicion se realiza lo siguiente:

0. Se definen las variables que van a ser introducidas a la consulta.

1. Se realiza una copia del dataframe original que e usara en esta consulta.

2. Para el valor `platfom`, se realiza el correspondiente filtrado.

3. Se obtiene el nombre de la plataforma a travez del diccionario creado anteriormente.

__Se genera la quinta ruta, la cual contiene la consulta `prod_per_county`.__

```python
def prod_per_county(type: str, country: str, year: int):
    temp_df = sp_dataset
    temp_df = temp_df[temp_df.release_year == year]
    temp_df = temp_df[temp_df.country == country.lower()]
    temp_df = temp_df[temp_df.type == type.lower()]

    return {
        "Pais": country.capitalize(),
        "Anio": year,
        "Peliculas": int(temp_df.shape[0])
    }
```

En la cual se desarrolla lo siguiente:

0. Se definen las variables que van a ser introducidas en la consulta

1. Se crea una copia del data set original.

2. Se filtra el contenido por el anio que haya sido ingresado.

3. Se filtra segun el pais.

4. Se filtra segun el tipo de contenido que se especifique.

5. Se retorna un diccionario con los valores correstpondientes a la cantidad de contenido de un tipo especifico, en un anio y pais dados.

__Se genera la sexta ruta, la cual contiene la consulta `get_contents`.__

```python
def get_contents(rating: str):
    temp_df = sp_dataset
    temp_df = temp_df[temp_df.rating == rating.lower()]

    return int(temp_df.shape[0])
```

A lo largo de esta definicion se desarrolla lo siguiente:

0. Se define la variable de entrada `rating`.

1. Se hace una copia del dataset original.

2. Se filtra el _rating_ especificado.

3. Se retorna la cantidad de shows que satisfacen el filtro anterior.

__Se genera la sexta ruta, la cual llama a un ___sistema de recomendacion___ el cual se anida bajo la consulta `get_recommendation`.__

```python
def get_recommendation(titulo: str):
    temp_df = big_df[["title", "mean"]]
    X = temp_df[["mean"]].values
    scaler = StandardScaler()
    X_std = scaler.fit_transform(X)

    k = 5
    knn = NearestNeighbors(n_neighbors=k, algorithm="ball_tree")
    knn.fit(X_std)
    movie_index = temp_df.title.isin([titulo]).idxmax()
    distances, indices = knn.kneighbors(X_std[movie_index].reshape(1, -1))
    reco = []
    for i in range(0, len(distances[0])):
        idx = indices[0][i]
        reco.append([temp_df.iloc[idx]["title"], temp_df.iloc[idx]["mean"]])
    reco = sorted(reco, key=lambda x: x[1], reverse=True)
    reco = [peli for el in reco for peli in el if isinstance(peli, str)]
    return reco
```

El anterior fragmento de codigo funciona de la siguiente manera:

0. Se declara la variable de entrada `titulo`.

1. Se duplican las columnas `title` y `mean` del dataset original.

2. 

---

## EDA

Se levantan los archivos `csv` creados en el proceso de ___ETL___, se les aplican algunas transformaciones adicionales.

1. En el dataframe `scores_df` se cambia el nombre de la columna `movieId` por `id`.

2. Se genera una agrupacion que permite obtener el `score` promedio para cada `id`.

3. Se unen los dos dataframes a través de la columna `id`.

4. Finalmente, se remplazan los valores `season` presentes en `duration_type` por `seasons`.

Se procede a realizar un reporte en el que es posible analizar las diferentes interacciones que tienen entre los datos presentes en `big_df`.

> Para visualizar el informe consultar el archivo `EDA.html`.
