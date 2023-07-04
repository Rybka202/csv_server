import pandas
from fastapi import HTTPException
import os
from csv import Sniffer
from database import conn
from sqlalchemy import select
from models import file_csv

def split_func(constraint: str):
    sep_str = []
    start = 0
    for i in range(1, len(constraint)):
        if constraint[i] in [' ', '>', '<', '=', '!'] and len(sep_str) == 0:
            sep_str.append(constraint[start:i])
        if start == 0 and constraint[i] in ['>', '<', '=', '!'] and len(sep_str) == 1:
            start = i
        if start != 0 and constraint[i] not in ['>', '<', '=', '!'] and len(sep_str) == 1:
            sep_str.append(constraint[start:i])
        if start != 0 and constraint[i] not in [' ', '>', '<', '=', '!'] and len(sep_str) == 2:
            sep_str.append(constraint[i:])
    return sep_str

def infoAllCSV(dirPath: str):
    csv_files = [file for file in os.listdir(dirPath) if file.endswith('.csv')]
    list_csv = []
    for file in csv_files:
        dict_csv = {}
        dataframe = pandas.read_csv(f"{dirPath}/{file}", sep='[,;]', engine='python')
        dataframe.rename(columns=lambda x: x.replace('"', '').replace(':', ''), inplace=True)
        query = select(file_csv.c.fileName).where(file_csv.c.id == file)
        result = conn.execute(query)
        dict_csv['fileName'] = f'{file} ({result.scalars().all()[0]})'
        columns = dataframe.columns.tolist()
        types = [str(dataframe.dtypes[column]) for column in columns]
        for i in range(len(types)):
            if types[i] == 'int64':
                types[i] = 'int'
            elif types[i] == 'float64':
                types[i] = 'float'
            else:
                types[i] = 'str'
        dict_csv['columns'] = [f"{columns[i]} ({types[i]})" for i in range(len(columns))]
        list_csv.append(dict_csv)
    return list_csv

def filtrate_df(dataframe, constraints: list[str]):
    if constraints == None:
        return dataframe
    dataframe.rename(columns=lambda x: x.replace('"', '').replace(':', ''), inplace=True)
    col_list = dataframe.columns.tolist()
    for const in constraints:
        const = split_func(const)
        if len(const) != 3:
            raise HTTPException(status_code=400, detail='Is not available to parse the query')
        try:
            col_list.index(const[0])
        except ValueError:
            raise HTTPException(status_code=400, detail=f'{const[0]} is not a column')
        type = dataframe.dtypes[const[0]]
        if type == 'int64':
            type = int
        elif type == 'float64':
            type = float
        else:
            type = str
        if (const[1] == '=='):
            dataframe = dataframe[dataframe[const[0]].isin(list(map(type, const[2].replace(' ', '').split(','))))]
        if (const[1] == '<='):
            dataframe = dataframe[dataframe[const[0]] <= type(const[2])]
        if (const[1] == '>='):
            dataframe = dataframe[dataframe[const[0]] >= type(const[2])]
        if (const[1] == '>'):
            dataframe = dataframe[dataframe[const[0]] > type(const[2])]
        if (const[1] == '<'):
            dataframe = dataframe[dataframe[const[0]] < type(const[2])]
        if (const[1] == '!='):
            dataframe = dataframe[~dataframe[const[0]].isin(list(map(type, const[2].replace(' ', '').split(','))))]

    return dataframe

def delimiterOfCSV(filePath: str):
    with open(filePath, 'r') as csvfile:
        dialect = Sniffer().sniff(csvfile.readline())
    return dialect.delimiter

def sort_df(dataframe, columns: list[str], asc: bool):
    if columns == None:
        return dataframe
    dataframe.rename(columns=lambda x: x.replace('"', '').replace(':', ''), inplace=True)
    col_list = dataframe.columns.tolist()
    for col in columns:
        try:
            col.strip()
            col_list.index(col)
        except ValueError:
            raise HTTPException(status_code=400, detail=f'{col} is not a column')
    return dataframe.sort_values(by=columns, ascending=asc)
