# Definicion de funciones

A continuacion se definen las funciones que permiten la creacion y manipulacion de los conjuntos de datos utilizados en la primera etapa del proyecto.

Esta funcion permite crear una lista con los nombres de los archivos `csv` presentes en un directorio

 ```python
 def getFileNames(path:str):
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
def joinCSV(path:str, files:list):
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

La siguiente funcion recibe una lista de dataframes para unirlos en uno solo

```python
def joinDF(l:list):
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
def getPrefix(l:str):
    return l[0]
```

Con la siguiente funcion se crea el `id` que combina los valores del campo `show_Id` y el resutado de la funcion descrita anteriormente

```python
def idGenerator(df, prefix:str):
   print('idGenerator')
   for e in range(len(df)):
       df['id'] = prefix+df.iloc[:, 0]
   return df
```

La siguiente funcion levanta un dataframe a partir de un `path` entregado y el nombre del archivo `csv`

```python
def getDF(path:str, name:str):
    df = pd.read_csv(path+name)
    prefix = getPrefix(name)
    print(prefix)
    a = idGenerator(df, prefix)
    return a
```

`convert2date(date)` recibe una cadena de caracteres a la cual le hace un split para separarla en diferentes componentes. Devuelve una fecha de tipo `date` en el formato YYYY-mm-dd

```python
def convert2date(date:str):
    try:
        months = {
            'january'   : '01',
            'february'  : '02',
            'march'     : '03',
            'april'     : '04',
            'may'       : '05',
            'june'      : '06',
            'july'      : '07',
            'august'    : '08',
            'septemeber': '09',
            'october'   : '10',
            'november'  : '11',
            'december'  : '12',
        }
        splt = date.split(' ')
        m = months[splt[0].lower()]
        d = splt[1]
        a = splt[2]
        date = a + '-' + m + '-' + d
        date = datetime.strptime(date, '%Y-%m-%d,')
        
        return date.date()
    except:
        'null value'
```
