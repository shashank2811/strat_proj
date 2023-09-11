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
import pandas as pd


def get_spot_close_price(row,entry_time, bnifty_df):
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

    # Extract the spot price if data is found, otherwise return NaN
    if not filtered_data.empty:
        spot_price = filtered_data.iloc[0]["tr_close"]
    else:
        spot_price = float("nan")  # Use NaN as a placeholder for missing data

    return spot_price


def get_target_stoploss_spotprice(entry_time, week_expiry, bnifty_df):
    """
    Get filtered data based on specific conditions and matching dates.

    Parameters:
        matching_dates (list): List of dates to filter data.
        Matching time ,weekexpiry and date from the spot dataframe and grouping
        all the data based on otype and weekexpiry and ,
        it selects the smallest (in ascending order) row based on the 'tr_close' column
        and finding the smallestvalue from that dataframe and returning it.

    Returns:
        DataFrame: Filtered data with columns target and stoploss
    """
    find_target_stoploss = (
        fnoieddf.loc[
            (fnoieddf["tr_time"] == entry_time)
            & (fnoieddf["week_expiry"] == week_expiry)
        ]
        .groupby(["tr_date", "week_expiry", "otype"])
        .apply(lambda group: group[group["tr_close"] >= 200].nsmallest(1, "tr_close"))
        .reset_index(drop=True)
    )
    find_target_stoploss["entry_price"] = find_target_stoploss["tr_close"]
    find_target_stoploss = find_target_stoploss.assign(
        target=find_target_stoploss["entry_price"] * 0.5,
        stoploss=find_target_stoploss["entry_price"] * 1.5,
    )
    find_target_stoploss["spot_price"] = find_target_stoploss.apply(
        get_spot_close_price, axis=1, bnifty_df=bnifty_df, entry_time=entry_time
    )

    return find_target_stoploss


def apply_exit_conditions(
    tr_date,
    week_expiry,
    strike_price,
    otype,
    target,
    stoploss,
    entry_time,
    squareoff_time,
):
    """
    Apply exit conditions and calculate exit parameters for a given row.

    Parameters:
        tr_date (str): Trading date.
        tr_time (str): Trading time.
        week_expiry (int): Week's expiry.
        strike_price (float): Strike price.
        otype (str): Option type.
        target (float): Target price.
        stoploss (float): Stoploss price.
        entry_time (str): Entry time.
        exit_type (str, optional): Exit type (TARGET, STOPLOSS, SQOFF). Defaults to None.
        exit_price (float, optional): Exit price. Defaults to None.
        exit_time (str, optional): Exit time. Defaults to None.

    Returns:
        tuple: Exit type, exit price, exit time.
    """

    stoploss_target_sqoff_filter = fnoieddf.loc[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] > entry_time)
        & (fnoieddf["tr_time"] <= squareoff_time)
        & (fnoieddf["week_expiry"] == week_expiry)
        & (fnoieddf["strike_price"] == strike_price)
        & (fnoieddf["otype"] == otype)
        & ((fnoieddf["tr_low"] <= target) | (fnoieddf["tr_high"] >= stoploss))
    ]
    exit_type = None
    exit_price = None
    exit_time = None
    if not stoploss_target_sqoff_filter.empty:
        stoploss_target_sqoff = stoploss_target_sqoff_filter.iloc[
            0
        ]  # Take the first matching row

        if stoploss_target_sqoff["tr_time"] > entry_time:
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


def main():
    """
    Main function to execute the data processing and filtering.

    Reads configuration, processes data, and saves the filtered data to a CSV file.
    """
    bnifty_df = pd.read_csv("spot_data.csv")
    lotsize_df.rename(columns={"Date": "tr_date"}, inplace=True)
    lotsize_df["tr_date"] = pd.to_datetime(lotsize_df["tr_date"]).dt.strftime(
        "%d-%m-%Y"
    )
    lotsize_df.rename(columns={"BankNifty": "Lot_Size"}, inplace=True)
    fnoieddf["tr_time"] = fnoieddf["tr_time"].astype(str)
    fnoieddf["tr_date"] = pd.to_datetime(fnoieddf["tr_date"]).dt.strftime("%d-%m-%Y")
    # Step 1: Find the target ,stoploss and spotprice
    find_exit_conditions = get_target_stoploss_spotprice(
        ENTRY_TIME, WEEK_EXPIRY, bnifty_df
    )
    # Step 2: Filter the data and find the exit conditions
    find_exit_conditions[
        ["exit_type", "exit_price", "exit_time"]
    ] = find_exit_conditions.apply(
        lambda row: apply_exit_conditions(
            row["tr_date"],
            WEEK_EXPIRY,
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
    # Step 3 : Convert it into a CSV file
    columns_to_drop = [
        "tr_open",
        "tr_high",
        "tr_low",
        "tr_close",
        "week_expiry",
        "expiry_date",
        "month_expiry",
        "tr_segment",
    ]
    find_exit_conditions = find_exit_conditions.drop(
        columns=columns_to_drop, errors="ignore"
    )
    output_csv_path = "S0001_inputfile.csv"
    find_exit_conditions.to_csv(output_csv_path, index=False)


if __name__ == "__main__":
    # Read the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    ENTRY_TIME = str(config.get("params", "entry_time"))
    WEEK_EXPIRY = int(config.get("params", "week_expiry"))
    SQUAREOFF_TIME = str(config.get("params", "squareoff_time"))

    fnoieddf = pd.read_excel("FNO_DATA.xlsx")
    lotsize_df = pd.read_csv("LotSize_Data.csv")
    main()
