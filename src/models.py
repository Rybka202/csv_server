from sqlalchemy import MetaData, Table, Column, String

metadata = MetaData()

file_csv = Table(
    "file_csv",
    metadata,
    Column("id", String, primary_key=True),
    Column("fileName", String, nullable=False)
)
