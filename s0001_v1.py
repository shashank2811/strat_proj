"""
Options Trading Script

This script performs options trading data processing, filtering, and analysis
based on specific conditions
including target, stoploss, and spot price calculations.

It reads configuration from 'config.ini', processes data from
'FNO_DATA.xlsx', 'spot_data.csv', and 'LotSize_Data.csv',
and saves the filtered data to a CSV file.

Author: B Shashank
Date: September 07, 2023
"""
import configparser
import json
import pandas as pd


# Function to get entry price based on provided conditions
def get_entry_price(tr_date, tr_time, otype, strike_price):
    """
    Get the entry price based on provided conditions.
    Parameters:
        tr_date (str): The transaction date.
        tr_time (str): The transaction time.
        otype (str): The option type (CE or PE).
        strike_price (float): The strike price.
    Returns:
        float or None: The entry price if found, otherwise None.
    """
    filtered_data = fnoieddf[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] == tr_time)
        & (fnoieddf["otype"] == otype)
        & (fnoieddf["strike_price"] == strike_price)
    ]
    if not filtered_data.empty:
        spot_price = filtered_data.iloc[0]["tr_close"]
    else:
        spot_price = float("nan")  # Use NaN as a placeholder for missing data

    return spot_price


# Function to apply exit conditions and calculate exit parameter
def apply_exit_conditions(
    tr_date,
    strike_price,
    otype,
    target,
    stoploss,
    entry_time,
    squareoff_time,
):
    """
    Apply exit conditions and calculate exit parameters.

    Parameters:
        tr_date (str): The transaction date.
        tr_time (str): The transaction time.
        strike_price (float): The strike price.
        otype (str): The option type (CE or PE).
        target (float): The target price.
        stoploss (float): The stoploss price.
        entry_time (str): The entry time.
        exit_type (str, optional): The exit type.
        exit_price (float, optional): The exit price.
        exit_time (str, optional): The exit time.

    Returns:
        tuple: A tuple containing exit_type, exit_price, and exit_time.
    """
    stoploss_target_sqoff_filter = fnoieddf.loc[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] > entry_time)
        & (fnoieddf["tr_time"] <= squareoff_time)
        & (fnoieddf["strike_price"] == strike_price)
        & (fnoieddf["otype"] == otype)
        & ((fnoieddf["tr_low"] <= target) | (fnoieddf["tr_high"] >= stoploss))
    ]
    exit_type = None
    exit_price = None
    exit_time = None
    if not stoploss_target_sqoff_filter.empty:
        stoploss_target_sqoff = stoploss_target_sqoff_filter.iloc[0]

        if stoploss_target_sqoff["tr_low"] <= target:
            exit_type = "TARGET"
            exit_price = target
            exit_time = stoploss_target_sqoff["tr_time"]
        elif stoploss_target_sqoff["tr_high"] >= stoploss:
            exit_type = "STOPLOSS"
            exit_price = stoploss
            exit_time = stoploss_target_sqoff["tr_time"]
        if stoploss_target_sqoff["tr_time"] == squareoff_time:
            exit_type = "SQOFF"
            exit_price = stoploss_target_sqoff["tr_close"]
            exit_time = squareoff_time

    return exit_type, exit_price, exit_time


# Function to prepare bankNifty DataFrame
def prepare_bank_nifty(bnifty_df, entry_time):
    """Combines transaction time twice to get a column of time to
    match with CE and PE and
    then it assigns otype CE and PE to all the available transaction dates
    And Strike price is rounded to nearest value and the corresponding
    first value of trclose is stored as entry price.

    Returns entry price
    """
    bank_nifty = pd.concat(
        [bnifty_df[bnifty_df["tr_time"] == entry_time]] * 2, ignore_index=True
    )
    bank_nifty = bank_nifty.assign(
        otype=(["CE", "PE"] * (len(bank_nifty) // 2 + 1))[: len(bank_nifty)],
        strike_price=((bank_nifty["tr_close"] // 100) * 100),
    )
    bank_nifty["entry_price"] = bank_nifty.apply(
        lambda row: get_entry_price(
            row["tr_date"],
            row["tr_time"],
            row["otype"],
            row["strike_price"],
        ),
        axis=1,
    )
    return bank_nifty


# Function to calculate target and stoploss columns
def calculate_target_stoploss(stoploss_value, target_value, target_df):
    """It calculates the target and stoploss values"""
    target_df = target_df.assign(
        target=target_df["entry_price"] * target_value,
        stoploss=target_df["entry_price"] * stoploss_value,
    )
    return target_df


# Function to add lot size column to DataFrame
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
    return fnoied_df


def main():
    """
    Main function to execute the data processing and filtering.
    Reads configuration, processes data, and saves the filtered
      data to a CSV file.
    """
    # reading the csv and excel file
    lotsize_df = pd.read_csv("LotSize_Data.csv")
    bnifty_df = pd.read_csv("spot_data.csv")
    # Preprocessing
    bnifty_df["tr_time"] = bnifty_df["tr_time"].astype(str)
    fnoieddf["tr_time"] = fnoieddf["tr_time"].astype(str)
    fnoieddf["tr_date"] = pd.to_datetime(fnoieddf["tr_date"]).dt.strftime("%d-%m-%Y")
    lotsize_df.rename(columns={"Date": "tr_date"}, inplace=True)
    lotsize_df["tr_date"] = pd.to_datetime(lotsize_df["tr_date"]).dt.strftime(
        "%d-%m-%Y"
    )
    lotsize_df.rename(columns={"BankNifty": "Lot_Size"}, inplace=True)
    # Getting stoploss target values from config
    stoploss_target_combo = json.loads(TARGET_STOPLOSS_VALUES)
    stoploss_value = stoploss_target_combo[0][0]
    target_value = stoploss_target_combo[0][1]

    # Step 1 and 2: Prepare bankNifty DataFrame
    bank_nifty_df = prepare_bank_nifty(bnifty_df, ENTRY_TIME)

    # Step 3: Calculate target and stoploss columns
    bank_nifty_df = calculate_target_stoploss(
        stoploss_value, target_value, bank_nifty_df
    )

    # Step 4: Apply exit conditions
    bank_nifty_df[["exit_type", "exit_price", "exit_time"]] = bank_nifty_df.apply(
        lambda row: apply_exit_conditions(
            row["tr_date"],
            row["strike_price"],
            row["otype"],
            row["target"],
            row["stoploss"],
            ENTRY_TIME,
            SQUAREOFF_TIME,
        ),
        axis=1,
        result_type="expand",
    )

    # Step 5: Add lot size column and prepare final DataFrame
    bank_nifty_df = add_lotsize_column(lotsize_df, bank_nifty_df)
    columns_to_drop = [
        "tr_open",
        "tr_high",
        "tr_low",
        "week_expiry_date",
        "expiry_date",
        "tr_segment",
    ]
    bank_nifty_df = bank_nifty_df.drop(columns=columns_to_drop, errors="ignore")
    # Step 6: Adding Profit and Loss Column
    bank_nifty_df["PNL"] = (
        bank_nifty_df["entry_price"] - bank_nifty_df["exit_price"]
    ) * bank_nifty_df["lotsize"]
    # Save final DataFrame to CSV
    output_csv_path = "S0001_v1.csv"
    bank_nifty_df.to_csv(output_csv_path, index=False)


if __name__ == "__main__":
    # Read the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    ENTRY_TIME = str(config.get("params", "entry_time"))
    WEEK_EXPIRY = int(config.get("params", "week_expiry"))
    SQUAREOFF_TIME = str(config.get("params", "squareoff_time"))
    TARGET_STOPLOSS_VALUES = config.get("params", "stoploss_target_combo")
    TR_SEGMENT = int(config.get("params", "tr_segment"))

    EXCEL_FILE_PATH = "FNO_DATA.xlsx"
    fnoieddf = pd.read_excel(EXCEL_FILE_PATH)
    fnoieddf = fnoieddf[
        (fnoieddf["week_expiry"] == WEEK_EXPIRY)
        & (fnoieddf["tr_segment"] == TR_SEGMENT)
    ]
    main()
