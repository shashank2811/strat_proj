from sqlalchemy import create_engine, Column, DateTime, Float, String, Integer, MetaData, Table
import pandas as pd

# Replace these values with your actual database connection details
username = 'root'
password = 'shashank23'
host = 'localhost'
port = '3306'
database = 'backtest'
table_name = 'strategy_1'

# SQLAlchemy Setup
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}', echo=True)
metadata = MetaData()

# Define the SQLAlchemy Table
table = Table(
    table_name,
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('Strategy_Name', String(length=255)),
    Column('Expiry_Date', DateTime),
    Column('Stock', String(length=255)),  # Adjust the length as needed
    Column('Stock_Name', String(length=255)),
    Column('Strike_Price', Float),
    Column('CE_PE', String(length=255)),
    Column('Trade_Type', String(length=255)),
    Column('Entry_Date', DateTime),
    Column('Entry_Time', String(length=255)),
    Column('Entry_Price', Float),
    Column('Exit_Date', DateTime),
    Column('Exit_Time', String(length=255)),
    Column('Exit_Price', Float),
    Column('Cycle_Id', String(length=255))
    # Add more columns as needed
)

# Create the table in the database (if not exists)
metadata.create_all(engine)
# Read data from Excel file
excel_file_path = 's0005_v1_bb_new_Nov_15_11_4.xlsx'
df = pd.read_excel(excel_file_path, sheet_name='Summary')

try:
    # Insert data into the database
    with engine.connect() as connection:
        for index, row in df.iloc[1:].iterrows():
            insert_statement = table.insert().values(
                Strategy_Name=row['Strategy_Name'],
                Expiry_Date=pd.to_datetime(row['Expiry_Date'], format='%d-%m-%Y'),
                Stock=row['Stock'],
                Stock_Name=row['Stock_Name'],
                Strike_Price=row['Strike_Price'],
                CE_PE=row['CE_PE'],
                Trade_Type=row['Trade_Type'],
                Entry_Date=pd.to_datetime(row['Entry_Date'], format='%d-%m-%Y'),
                Entry_Time=row['Entry_Time'],
                Entry_Price=row['Entry_Price'],
                Exit_Date=pd.to_datetime(row['Exit_Date'], format='%d-%m-%Y'),
                Exit_Time=row['Exit_Time'],
                Exit_Price=row['Exit_Price'],
                Cycle_Id=row['Cycle_Id']
                # Add more values as needed
            )
            connection.execute(insert_statement)
        connection.commit()
except Exception as e:
    print(f"Error: {e}")
# Close the database connection
engine.dispose()
