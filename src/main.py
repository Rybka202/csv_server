from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse
from typing import List, Optional
from shutil import copyfileobj
import pandas
import os
import uuid
import handler
from database import conn
from sqlalchemy import insert, delete, select
from models import file_csv

app = FastAPI(
    title="CSV_server"
)


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    if not file.content_type.startswith('text/csv'):
        raise HTTPException(status_code=400, detail="Is not a csv file")
    fileName = str(uuid.uuid1())+'.csv'
    with open(f"./../storage_csv/{fileName}", "wb") as buf:
        copyfileobj(file.file, buf)
    stmt = insert(file_csv).values([fileName, file.filename])
    conn.execute(stmt)
    conn.commit()
    return {'New fileName': fileName}

@app.get("/getInfoAllCsvFiles")
async def getInfoAll():
    dirPath = "./../storage_csv"
    return handler.infoAllCSV(dirPath)

@app.get("/getFilterFile/{id}")
def getFilterFile(id: str, constraints: Optional[List[str]] = Query(default=None,
                                                                    description='''ColumnName1 == 1, 5, 6;
                                                                                        ColumnName2 >= 0.5;
                                                                                        ColumnName3 != hello''')):
    filePath = f"./../storage_csv/{id}"
    if not os.path.exists(filePath):
        raise HTTPException(status_code=400, detail='File Not Found')
    delimiter = handler.delimiterOfCSV(filePath)
    dataframe = pandas.read_csv(filePath, sep=delimiter, engine='python')
    try:
        dataframe = handler.filtrate_df(dataframe, constraints)
    except HTTPException as e:
        raise e
    dataframe.to_csv("filter.csv", index=False, quotechar=' ', sep=delimiter)
    return FileResponse(path='filter.csv', filename='filter.csv', media_type='application/octet-stream')


@app.get("/getSortFile/{id}")
def getSortFile(id: str, columns: Optional[List[str]] = Query(None), ascendingSort: bool = True):
    filePath = f"./../storage_csv/{id}"
    if not os.path.exists(filePath):
        raise HTTPException(status_code=400, detail='File Not Found')
    delimiter = handler.delimiterOfCSV(filePath)
    dataframe = pandas.read_csv(filePath, sep=delimiter, engine='python')
    try:
        dataframe = handler.sort_df(dataframe, columns, ascendingSort)
    except HTTPException as e:
        raise e
    dataframe.to_csv("sort.csv", index=False, quotechar=' ', sep=delimiter)
    return FileResponse(path='sort.csv', filename='sort.csv', media_type='application/octet-stream')

@app.delete("/deleteFile/{id}")
def deleteFile(id: str):
    filepath = f"./../storage_csv/{id}"
    if not os.path.exists(filepath):
        raise HTTPException(status_code=400, detail='File Not Found')
    os.remove(filepath)
    del_query = delete(file_csv).where(file_csv.c.id == id)
    conn.execute(del_query)
    conn.commit()
    return f"{id} was delete successfully"