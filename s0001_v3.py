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
import sys
import pandas as pd



def get_spot_close_price(row, bnifty_df, entry_time_or_new_entry_time, mode):
    """
    Get spot price for a specific date and entry time.

    Parameters:
        row (Series): A row from the target DataFrame containing 'tr_date' and 'tr_time'.
        bnifty_df (DataFrame): DataFrame containing spot data.
        entry_time_or_new_entry_time (str): The entry time to filter dates.
        mode (str): Either 'old' or 'new' to indicate which mode to use.

    Returns:
        float: Spot price matching the provided date and entry time.
    """

    if mode == "old":
        # Use loc to filter spot data for the specific date and entry time
        filtered_data = bnifty_df.loc[
            (bnifty_df["tr_date"] == row["tr_date"])
            & (bnifty_df["tr_time"] == entry_time_or_new_entry_time)
        ]
    elif mode == "new":
        # Use loc to filter spot data for the specific date and new entry time
        filtered_data = bnifty_df.loc[
            (bnifty_df["tr_date"] == row["tr_date"])
            & (bnifty_df["tr_time"] == entry_time_or_new_entry_time)
        ]

    # Extract the spot price if data is found, otherwise return NaN
    if not filtered_data.empty:
        spot_price = filtered_data.iloc[0]["tr_close"]
    else:
        spot_price = float("nan")  # Use NaN as a placeholder for missing data

    return spot_price


def get_target_stoploss_spotprice(
    entry_time_or_new_entry_time, week_expiry, bnifty_df, mode, tr_date=None, otype=None
):
    """
    Get filtered data based on specific conditions and matching dates.

    Parameters:
        entry_time_or_new_entry_time (str): The entry time to filter dates.
        week_expiry (int): Week's expiry.
        bnifty_df (DataFrame): DataFrame containing spot data.
        mode (str): Either 'old' or 'new' to indicate which mode to use.
        tr_date (str, optional): The trading date to filter (only used in 'new' mode).
        otype (str, optional): The option type to filter (only used in 'new' mode).

    Returns:
        DataFrame: Filtered data with columns target and stoploss
    """
    if mode == "old":
        # Use the provided entry_time
        find_target_stoploss = (
            fnoieddf.loc[
                (fnoieddf["tr_time"] == entry_time_or_new_entry_time)
                & (fnoieddf["week_expiry"] == week_expiry)
            ]
            .groupby(["tr_date", "week_expiry", "otype"])
            .apply(
                lambda group: group[group["tr_close"] >= 200].nsmallest(1, "tr_close")
            )
            .reset_index(drop=True)
        )
        find_target_stoploss["entry_price"] = find_target_stoploss["tr_close"]
        find_target_stoploss = find_target_stoploss.assign(
            target=find_target_stoploss["entry_price"] * 0.5,
            stoploss=find_target_stoploss["entry_price"] * 1.5,
        )
        find_target_stoploss["spot_price"] = find_target_stoploss.apply(
            lambda row: get_spot_close_price(
                row, bnifty_df, entry_time_or_new_entry_time, mode
            ),
            axis=1,
        )
    elif mode == "new":
        # Use the provided new_entry_time, tr_date, and otype
        find_target_stoploss = (
            fnoieddf.loc[
                (fnoieddf["tr_time"] == entry_time_or_new_entry_time)
                & (fnoieddf["week_expiry"] == week_expiry)
                & (fnoieddf["tr_date"] == tr_date)
                & (fnoieddf["otype"] == otype)
            ]
            .groupby(["tr_date", "week_expiry", "otype"])
            .apply(
                lambda group: group[group["tr_close"] >= 200].nsmallest(1, "tr_close")
            )
            .reset_index(drop=True)
        )
        find_target_stoploss["entry_price"] = find_target_stoploss["tr_close"]
        find_target_stoploss = find_target_stoploss.assign(
            target=find_target_stoploss["entry_price"] * 0.5,
            stoploss=find_target_stoploss["entry_price"] * 1.5,
        )
        find_target_stoploss["spot_price"] = find_target_stoploss.apply(
            lambda row: get_spot_close_price(
                row, bnifty_df, entry_time_or_new_entry_time, mode
            ),
            axis=1,
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
    mode="old",
):
    """
    Apply exit conditions and calculate exit parameters for a given row.

    Parameters:
        tr_date (str): Trading date.
        week_expiry (int): Week's expiry.
        strike_price (float): Strike price.
        otype (str): Option type.
        target (float): Target price.
        stoploss (float): Stoploss price.
        entry_time (str): Entry time.
        squareoff_time (str): Squareoff time.
        mode (str, optional): Mode of operation, either 'old' or 'new'. Defaults to 'old'.

    Returns:
        tuple: Exit type, exit price, exit time.
    """

    if mode == "old":
        time_to_filter = entry_time
    elif mode == "new":
        time_to_filter = entry_time
    else:
        raise ValueError("Invalid mode. Use 'old' or 'new'.")

    stoploss_target_sqoff_filter = fnoieddf.loc[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] > time_to_filter)
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
        stoploss_target_sqoff = stoploss_target_sqoff_filter.iloc[0]

        if stoploss_target_sqoff["tr_time"] > time_to_filter:
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
    fnoieddf["tr_time"] = fnoieddf["tr_time"].astype(str)
    fnoieddf["tr_date"] = pd.to_datetime(fnoieddf["tr_date"]).dt.strftime("%d-%m-%Y")

    # Initialize an empty DataFrame to store the results
    find_exit_conditions = get_target_stoploss_spotprice(
        ENTRY_TIME, WEEK_EXPIRY, bnifty_df, mode="old"
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
    print(find_exit_conditions)
    # Step 3 Find stoploss rows and clasiify it has stoploss r target based on below conditions
    stoploss_rows = find_exit_conditions[
        (find_exit_conditions["exit_type"] == "STOPLOSS")
    ]
    if stoploss_rows.empty:
        sys.exit()
    else:
        # Take the exit_time from the first 'STOPLOSS' row as the new entry_time
        for _, stoploss_row in stoploss_rows.iterrows():
            new_entry_time = stoploss_row["exit_time"]
            new_tr_date = stoploss_row["tr_date"]
            new_otype = stoploss_row["otype"]

            # Get data using the new entry time, tr_date, and otype
            find_stoploss_rows = get_target_stoploss_spotprice(
                new_entry_time,
                WEEK_EXPIRY,
                bnifty_df,
                mode="new",
                tr_date=new_tr_date,
                otype=new_otype,
            )
            find_exit_conditions = pd.concat([find_exit_conditions, find_stoploss_rows])
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
                    row["tr_time"],
                    SQUAREOFF_TIME,
                ),
                axis=1,
                result_type="expand",
            )
    
    columns_to_drop = ['tr_open', 'tr_high', 'tr_low', 'tr_close', 'week_expiry','expiry_date','month_expiry', 'tr_segment']
    find_exit_conditions = find_exit_conditions.drop(columns=columns_to_drop, errors='ignore')
    # Save the DataFrame to a CSV file if needed
    find_exit_conditions.to_csv("S0001_v3.csv", index=False)


if __name__ == "__main__":
    # Read the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    ENTRY_TIME = str(config.get("params", "entry_time"))
    WEEK_EXPIRY = int(config.get("params", "week_expiry"))
    SQUAREOFF_TIME = str(config.get("params", "squareoff_time"))
    # reading the csv and excel file
    EXCEL_FILE_PATH = "FNO_DATA.xlsx"
    fnoieddf = pd.read_excel(EXCEL_FILE_PATH)
    lotsize_df = pd.read_csv("LotSize_Data.csv")
    main()
