"""
Options Trading Script

This script performs options trading data processing, filtering, and analysis 
based on specific conditions
including target, stoploss, and spot price calculations.

It reads configuration from 'config.ini', processes data from 
'indices_spot_ieod' and  'fnodata2019' databases and from 'LotSize_Data.csv' excel file,
and saves the filtered data to a CSV file.

Author: B Shashank
Date: October 06, 2023
"""
import configparser
import json
import datetime
import tempfile
import psycopg2
import numpy as np
import pandas as pd
from rich.console import Console

console = Console()


def _query_db(dbname, query, verbose=True):
    """
    Query a PostgreSQL database and return the results as a Pandas DataFrame.

    Parameters:
        dbname (str): The name of the PostgreSQL database to connect to.
        query (str): The SQL query to execute.
        verbose (bool, optional): If True, print verbose connection and query information. Defaults to True.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the results of the SQL query.
    """
    if verbose:
        console.log(f"Connecting to [red on black]{dbname}[/]", style="bold green")
        console.log(f"Query: [magenta]{query}[/]")

    # Database connection parameters
    kwargs = {
        "host": "localhost",
        "database": dbname,
        "user": "backtestuser",
        "password": "BaCkTeSt@2019",
        "port": 5432,
    }

    with psycopg2.connect(**kwargs) as conn:
        with tempfile.TemporaryFile() as tmpfile:
            head = True  # Indicate whether to include CSV header
            # Construct the SQL command for copying data to a CSV file
            copy_sql = (
                f"COPY ({query}) TO STDOUT WITH CSV HEADER".format(query=query)
                if head
                else f"COPY ({query}) TO STDOUT WITH CSV".format(query=query)
            )
            cur = conn.cursor()
            cur.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            # Read the CSV data from the temporary file into a Pandas DataFrame
            db_results = pd.read_csv(tmpfile)

    # If the database name doesn't start with "fnodata" and verbose is True, print the results
    if not dbname.startswith("fnodata") and verbose:
        console.log(db_results)

    return db_results


def generate_date_range(start_date, end_date):
    """
    Generate a list of dates between start_date and end_date.
    """
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date)
        current_date += datetime.timedelta(days=1)
    return date_range

def get_spot_close_price(row, entry_time, bnifty_df):
    """
    Get spot price for a specific date and entry time.

    Parameters:
        row (Series): A row from the target DataFrame containing 'tr_date' and 'tr_time'.
        bnifty_df (DataFrame): DataFrame containing spot data.

    Returns:
        float: Spot price matching the provided date and entry time.
    """

    # Use loc to filter spot data for the specific date and entry time
    filtered_data = bnifty_df.loc[
        (bnifty_df["tr_date"] == row["tr_date"]) & (bnifty_df["tr_time"] == entry_time)
    ]

    if not filtered_data.empty:
        spot_price = filtered_data.iloc[0]["tr_close"]
        # Round the spot price to the nearest 100
        rounded_spot_price = np.round(spot_price / 100) * 100
    else:
        spot_price = np.nan  # Use NaN as a placeholder for missing data
        rounded_spot_price = np.nan  # Use NaN as a placeholder for missing data

    return spot_price, rounded_spot_price


def get_closest_strike_price(
 group, entry_time, closest_val,bnifty_df,trigger_val
):
    """
    Get filtered data based on specific conditions and matching dates.

    Parameters:
        group (DataFrame): DataFrame containing grouped data like otype and tr_date
        Matching entry_time, weekexpiry, and tr_segment
        and the filtered group is matched with tr_close value which is nearest to 200
        and the respective strike price is fetched from it

    Returns:
       int: Closest strike price based on the specified conditions.
    """   
    # Filter the 'group' DataFrame to include rows with 'entry_time'
    filtered_group = group[(group["tr_time"] == entry_time)]

    # Find the row with the minimum value
    closest_strike_row = min(
        filtered_group.itertuples(), key=lambda row: abs(row.tr_close - closest_val)
    )

    # Convert the closest row to a dictionary becoz i have used keys
    find_target_stoploss = closest_strike_row._asdict()
    find_target_stoploss["temp_entry_price"] = find_target_stoploss["tr_close"] * trigger_val

    # Convert the dictionary into a DataFrame using transpose
    find_target_stoploss = pd.DataFrame(find_target_stoploss, index=[0])
    find_target_stoploss[["spot_price", "strike_price"]] = find_target_stoploss.apply(
    lambda row: get_spot_close_price(row, entry_time, bnifty_df),
    axis=1,
    result_type="expand",
    )

    return find_target_stoploss

def apply_entry_time_conditions(
    tr_date,
    otype,
    entry_time,
    squareoff_time,
    temp_entry_price,
    strike_price,
    fnoieddf,

):
    """
    Apply entry time conditions and find the appropriate entry time for a given option trade.

    This function searches for a suitable entry time based on specified criteria within a DataFrame
    containing option trade data.

    Parameters:
        tr_date (str): The trading date of the option trade.
        otype (str): The type of option (e.g., 'CE' for Call Option or 'PE' for Put Option).
        entry_time (str): The desired entry time for the trade.
        squareoff_time (str): The square-off time for the trade.
        temp_entry_price (float): The temporary entry price for the option trade.
        strike_price (float): The strike price of the option.
        fnoieddf (DataFrame): A DataFrame containing option trade data with columns such as:
            - "tr_date" (str): Trading date.
            - "tr_time" (str): Trading time.
            - "otype" (str): Option type.
            - "strike_price" (float): Strike price.
            - "tr_low" (float): Low price during the trade.

    Returns:
        str or None: The calculated entry time that meets the specified conditions, or None if no suitable entry time is found.

    Notes:
        This function searches for a suitable entry time within the provided DataFrame (`fnoieddf`) based on the following criteria:
        - Matching trading date (`tr_date`).
        - Entry time falling between `entry_time` and `squareoff_time`.
        - Matching option type (`otype`).
        - Matching strike price (`strike_price`).
        - Low price during the trade (`tr_low`) less than or equal to `temp_entry_price`.

        If a matching entry time is found, it is returned; otherwise, None is returned to indicate that no suitable entry time was found.
    """
    print("NEW ENTRYYYYY TIMWe")
    print(tr_date,
    otype,
    entry_time,
    squareoff_time,
    temp_entry_price,
    strike_price)
    # finds the closest strike prices from the dataframe
    # print("STRIKE PRICE VALUES", closest_strike_price)
    find_entry_time = fnoieddf.loc[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] > entry_time)
        & (fnoieddf["tr_time"] < squareoff_time)
        & (fnoieddf["otype"] == otype)
        & (fnoieddf["strike_price"] == strike_price)
        & (fnoieddf["tr_low"]<=temp_entry_price)
    ]
    print("--------------------------------------------ENTRY-------FILTER------------------------")
    print(find_entry_time)
    entry_time_new = None
    if not find_entry_time.empty:
        entry_time_new = find_entry_time.iloc[0]["tr_time"]
    return entry_time_new

def apply_exit_conditions(
    tr_date, strike_price, otype,stoploss,target,entry_time, squareoff_time, fnoieddf
):
    """
    Apply exit conditions and calculate exit parameters for a given row.

    Parameters:
        tr_date (str): Trading date.
        strike_price (float): Strike price.
        otype (str): Option type.
        stoploss (float): Stoploss price.
        entry_time (str): Entry time.
        squareoff_time (str): Square-off time.
        fnoieddf (pd.DataFrame): A DataFrame containing option trade data with columns such as:
            - "tr_date" (str): Trading date.
            - "tr_time" (str): Trading time.
            - "tr_open" (float): Open price during the trade.
            - "tr_high" (float): High price during the trade.
            - "tr_low" (float): Low price during the trade.
            - "strike_price" (float): Strike price.
            - "otype" (str): Option type.

    Returns:
        tuple: A tuple containing exit type, exit price, and exit time.
    """

    stoploss_target_sqoff_filter = fnoieddf.loc[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] > entry_time)
        & (fnoieddf["tr_time"] <= squareoff_time)
        & (fnoieddf["strike_price"] == strike_price)
        & (fnoieddf["otype"] == otype)
        & ((fnoieddf["tr_low"] <= target) | (fnoieddf["tr_high"] >= stoploss))
    ]

    print(stoploss_target_sqoff_filter)
    if not stoploss_target_sqoff_filter.empty:
        stoploss_target_sqoff = stoploss_target_sqoff_filter.iloc[
            0
        ]  # Take the first matching row
        # print(stoploss_target_sqoff)
        if stoploss_target_sqoff["tr_high"] >= stoploss:
            exit_type = "STOPLOSS"
            exit_price = stoploss
            exit_time = stoploss_target_sqoff["tr_time"]
        elif stoploss_target_sqoff["tr_low"] <= target:
            exit_type = "TARGET"
            exit_price = target
            exit_time = stoploss_target_sqoff["tr_time"]
    if stoploss_target_sqoff_filter.empty:
        sqoff_row = fnoieddf.loc[
            (fnoieddf["tr_date"] == tr_date)
            & (fnoieddf["tr_time"] == squareoff_time)
            & (fnoieddf["strike_price"] == strike_price)
            & (fnoieddf["otype"] == otype)
        ]
        if not sqoff_row.empty:
            exit_type = "SQOFF"
            exit_price = sqoff_row.iloc[0]["tr_close"]
            exit_time = squareoff_time

    return exit_type, exit_price, exit_time

def add_lotsize_column(lotsize_df, fnoied_df):
    """It matches the dates from the resulting dataframe
      with the lotsize dataframe and
      takes the respective values of lotsize column
    and places it in the resulting data frame
    Returns lotsize column
    """
    filter_dates_list = fnoied_df["tr_date"].unique()
    lotsize_df = lotsize_df[lotsize_df["tr_date"].isin(filter_dates_list)]
    date_lotsize_mapping = dict(zip(lotsize_df["tr_date"], lotsize_df["Lot_Size"]))
    fnoied_df["lotsize"] = fnoied_df["tr_date"].map(date_lotsize_mapping)
    # print(fnoied_df)
    return fnoied_df

def process_data_for_date(bnifty_df):
    """
    Process and analyze data for a specific date.

    This function takes three DataFrames: `bnifty_df`, `fnoieddf`, and `lotsize_df`,
    and performs the following steps:

    1. Converts 'tr_time' columns in `bnifty_df` and `fnoieddf` to strings.
    2. Reads stoploss and target values from the configuration.
    3. Finds the spot close value for the specified entry time in `bnifty_df`.
    4. Groups data in `fnoieddf` by 'tr_date' and 'otype', and finds the closest strike price.
    5. Merges the spot close price with the strike price data to form `find_exit_conditions`.
    6. Calculates exit conditions using the `apply_exit_conditions` function.
    7. Adds a lot size column to `find_exit_conditions` using `lotsize_df`.
    8. Calculates profit and loss (PNL) based on exit conditions.
    9. Drops unnecessary columns from `find_exit_conditions`.

    Parameters:
        bnifty_df (DataFrame): DataFrame containing spot data.
        fnoieddf (DataFrame): DataFrame containing option data.
        lotsize_df (DataFrame): DataFrame containing lot size data.

    Returns:
        DataFrame: Processed and analyzed data for the specified date.
    """
    bnifty_df["tr_time"] = bnifty_df["tr_time"].astype(str)

    # Ensure that the "tr_date" column is in datetime format (if not already)
    bnifty_df["tr_date"] = pd.to_datetime(bnifty_df["tr_date"])

    # Set "tr_date" as the index (if not already)
    bnifty_df.set_index("tr_date", inplace=True)

    # Resample the data to a 5-minute frequency and calculate the mean closing price for each 5-minute interval
    resampled_data = bnifty_df.resample("5T")

    print("----------------------------------------------FINAL DATAFRAME------------------------------------------------------------------")
    
    return  resampled_data



def main():
    """
    Main function to execute the data processing and filtering.

    Reads configuration, processes data, and saves the filtered data to a CSV file.
    """
    start_date = datetime.datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(END_DATE, "%Y-%m-%d")

    # Generate a list of dates between start_date and end_date
    date_range = generate_date_range(start_date, end_date)

    # Create an empty list to accumulate DataFrames
    data_for_dates = []
    # Loop through each date and process data
    for date in date_range:
        # Process data for the current date
        query1 = f"""SELECT tr_date, tr_time, tr_close,stock_name,tr_segment
            FROM spot_indices_ieod_gdfl
            WHERE stock_name='{STOCK_NAME}' AND
            tr_date='{date.strftime('%Y-%m-%d')}'
            AND tr_time BETWEEN '09:15:59' AND '15:29:59' 
            ORDER BY tr_date,tr_time ASC"""
        
        db_results1 = _query_db("indices_spot_ieod", query1)

        # Create a DataFrame from the query results
        data_for_date = pd.DataFrame(db_results1)

        if data_for_date.empty:
            continue

        # Convert tr_date and tr_time to datetime objects
        data_for_date['tr_datetime'] = pd.to_datetime(data_for_date['tr_date'] + ' ' + data_for_date['tr_time'])

        # Set tr_datetime as the index
        data_for_date.set_index('tr_datetime', inplace=True)

        # Resample the data for 5-minute intervals
        resampled_data = data_for_date.resample('5T').last()  # Adjust 'last' to the desired resampling method

        # Append resampled_data to the list of DataFrames
        data_for_dates.append(resampled_data)

    # Concatenate all DataFrames into a single DataFrame
    s0004_v1_2019 = pd.concat(data_for_dates)

    # Print the resampled data
    print(s0004_v1_2019)
  
    output_csv_path = "S0004_v1_2019.csv"
    s0004_v1_2019.to_csv(output_csv_path, index=False)



if __name__ == "__main__":
    # Read the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    STOCK_NAME = str(config.get("params", "stock_name"))
    ENTRY_TIME = str(config.get("params", "entry_time"))
    WEEK_EXPIRY = int(config.get("params", "week_expiry"))
    SQUAREOFF_TIME = str(config.get("params", "squareoff_time"))
    TARGET_STOPLOSS_VALUES = config.get("params", "stoploss_target_combo")
    START_DATE = str(config.get("params", "start_date"))
    END_DATE = str(config.get("params", "end_date"))
    TR_SEGMENT = int(config.get("params", "tr_segment"))
    CLOSEST_VAL = int(config.get("params", "closest_val"))
    TRIGGER_VAL = float(config.get("params", "trigger_val"))

    main()