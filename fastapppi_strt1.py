from datetime import datetime
from fastapi import *
from io import BytesIO
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
import pandas as pd


# Replace these values with your actual database connection details
USERNAME = 'root'
PASSWORD= 'shashank23'
HOST = 'localhost'
PORT = '3306'
DATABASE = 'backtest'
TABLE_NAME = 'strategy_1'

# SQLAlchemy Setup
engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}', echo=True)
metadata = MetaData()
Session = sessionmaker(bind=engine)

# Define the SQLAlchemy Table
table = Table(
    TABLE_NAME,
    metadata,
    autoload_with=engine
)

app = FastAPI()
#ur just accepting the file so post method and 
# next is the url part of it http://127.0.0.1:8000/uploadfile/
# @app.post("/uploadfile/")
# async def create_upload_file(file: UploadFile):
#     return {"filename": file.filename}
@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    """
    Endpoint to upload an Excel file, read its content, and insert data into the database.

    Args:
        file (UploadFile): The Excel file to be uploaded.

    Returns:
        dict: A dictionary containing the uploaded filename.
    """
    try:
        print("Reading data from Excel file")
        # Read data from Excel file
        content = await file.read()
        excel_data = BytesIO(content)

        df = pd.read_excel(excel_data, sheet_name='Summary')
        print(df)

        print("Inserting data into the database")
        # Insert data into the database
        with engine.connect() as connection:
            for _, row in df.iloc[1:].iterrows():
                # Convert date strings to 'YYYY-MM-DD' format
                expiry_date_str=datetime.strptime(row['Expiry_Date'], '%d-%m-%Y').strftime('%Y-%m-%d')
                entry_date_str = datetime.strptime(row['Entry_Date'], '%d-%m-%Y').strftime('%Y-%m-%d')
                exit_date_str = datetime.strptime(row['Exit_Date'], '%d-%m-%Y').strftime('%Y-%m-%d')
                insert_statement = table.insert().values(
                    Strategy_Name=row['Strategy_Name'],
                    Expiry_Date=expiry_date_str,
                    Stock=row['Stock'],
                    Stock_Name=row['Stock_Name'],
                    Strike_Price=row['Strike_Price'],
                    CE_PE=row['CE_PE'],
                    Trade_Type=row['Trade_Type'],
                    Entry_Date=entry_date_str,
                    Entry_Time=row['Entry_Time'],
                    Entry_Price=row['Entry_Price'],
                    Exit_Date=exit_date_str,
                    Exit_Time=row['Exit_Time'],
                    Exit_Price=row['Exit_Price'],
                    Cycle_Id=row['Cycle_Id']
                )
                connection.execute(insert_statement)
            connection.commit()
        print("Data successfully inserted into the database")
    except Exception as e:
        print(f"Error processing file: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")

    return {"filename": file.filename}


@app.get("/get_data/")
async def get_data_by_date(date: str = Query(..., description="Enter date in format DD-MM-YYYY")):
    """
    Endpoint to retrieve data from the database based on the given date.

    Args:
        date (str): The date in the format DD-MM-YYYY.

    Returns:
        list: A list containing the data retrieved from the database.
    """
    try:
        formatted_date = datetime.strptime(date, '%d-%m-%Y').date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use DD-MM-YYYY.")

    with Session() as db:
        try:
            query = select(table).where(table.c.Entry_Date == formatted_date)
            print(f"Query: {query}")
            result = db.execute(query).fetchall()
            print(f"Result: {result}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error executing database query: {e}")

    if not result:
        raise HTTPException(status_code=404, detail=f"No data found for date: {formatted_date}")

    return {"data": [list(row) for row in result]}

