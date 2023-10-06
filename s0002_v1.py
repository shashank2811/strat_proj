"""
Options Trading Script

This script performs options trading data processing, filtering, and analysis 
based on specific conditions
including target, stoploss, and spot price calculations and 
finding exit status and returning the dataframe.

It reads configuration from 'config.ini', processes data from 
'FNO_DATA.xlsx', 'spot_data.csv', and 'LotSize_Data.csv',
and saves the filtered data to a CSV file.

Author: B Shashank
Date: September 15, 2023
"""
import configparser
import json
import pandas as pd


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
    filtered_data_spot_price = bnifty_df.loc[
        (bnifty_df["tr_date"] == row["tr_date"]) & (bnifty_df["tr_time"] == entry_time)
    ]
    # Extract the spot prices if data is found, otherwise return NaN
    print(filtered_data_spot_price)
    if not filtered_data_spot_price.empty:
        spot_price = filtered_data_spot_price.iloc[0]["tr_close"]
    else:
        spot_price = float("nan")  # Use NaN as a placeholder for missing data
    return spot_price


def get_closest_strike_price(
    stoploss_value, target_value, group, entry_time, closest_val
):
    """
    Get filtered data based on specific conditions and matching dates.

    Parameters:
        group (DataFrame): DataFrame containing grouped data like otype and tr_date
        Matching entry_time ,weekexpiry and tr_segment
        and the filtered group is matched with tr_close value which is nearest to 200
        and the respective strike price is fetched from it

    Returns:
       int: Closest strike price based on the specified conditions.
    """
    filtered_group = group[(group["tr_time"] == entry_time)]
    find_target_stoploss = filtered_group.loc[
        filtered_group["tr_close"].sub(closest_val).abs().idxmin()
    ]
    find_target_stoploss["entry_price"] = find_target_stoploss["tr_close"]
    print(find_target_stoploss)
    # converted series into a dataframe using transpose
    find_target_stoploss = pd.DataFrame(find_target_stoploss).transpose()
    find_target_stoploss = find_target_stoploss.assign(
        target=find_target_stoploss["entry_price"] * target_value,
        stoploss=find_target_stoploss["entry_price"] * stoploss_value,
    )
    find_target_stoploss["entry_type"] = "SELL"
    return find_target_stoploss


def apply_exit_conditions(
    tr_date, otype, target, stoploss, entry_time, squareoff_time, find_close_price
):
    """
    Apply exit conditions and calculate exit parameters for a given row.

    Parameters:
        tr_date (str): Trading date.
        strike_price (float): Strike price.
        otype (str): Option type.
        target (float): Target price.
        stoploss (float): Stoploss price.
        entry_time (str): Entry time.
        find_close_price: Dataframe for obtaining strike prices.
        exit_type (str, optional): Exit type (TARGET, STOPLOSS, SQOFF). Defaults to None.
        exit_price (float, optional): Exit price. Defaults to None.
        exit_time (str, optional): Exit time. Defaults to None.

    Returns:
        tuple: Exit type, exit price, exit time.
    """
    print(tr_date, otype, target, stoploss, entry_time, squareoff_time)
    # finds the closest strike prices from the dataframe
    closest_strike_price = find_close_price[
        (find_close_price["tr_date"] == tr_date) & (find_close_price["otype"] == otype)
    ]["strike_price"].values[0]
    print("STRIKE PRICE VALUES", closest_strike_price)
    stoploss_target_sqoff_filter = fnoieddf.loc[
        (fnoieddf["tr_date"] == tr_date)
        & (fnoieddf["tr_time"] > entry_time)
        & (fnoieddf["tr_time"] < squareoff_time)
        & (fnoieddf["strike_price"] == closest_strike_price)
        & (fnoieddf["otype"] == otype)
        & ((fnoieddf["tr_low"] <= target) | (fnoieddf["tr_high"] >= stoploss))
    ]
    print(stoploss_target_sqoff_filter)
    exit_type = None
    exit_price = None
    exit_time = None
    if not stoploss_target_sqoff_filter.empty:
        stoploss_target_sqoff = stoploss_target_sqoff_filter.iloc[
            0
        ]  # Take the first matching row
        print(stoploss_target_sqoff)
        if stoploss_target_sqoff["tr_low"] <= target:
            exit_type = "TARGET"
            exit_price = target
            exit_time = stoploss_target_sqoff["tr_time"]
        elif stoploss_target_sqoff["tr_high"] >= stoploss:
            exit_type = "STOPLOSS"
            exit_price = stoploss
            exit_time = stoploss_target_sqoff["tr_time"]
    if stoploss_target_sqoff_filter.empty:
        sqoff_row = fnoieddf.loc[
            (fnoieddf["tr_date"] == tr_date)
            & (fnoieddf["tr_time"] == squareoff_time)
            & (fnoieddf["strike_price"] == closest_strike_price)
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
    return fnoied_df


def main():
    """
    Main function to execute the data processing and filtering.

    Reads configuration, processes data, and saves the filtered data to a CSV file.
    """
    bnifty_df = pd.read_csv("spot_data.csv")
    lotsize_df = pd.read_csv("LotSize_Data.csv")
    lotsize_df.rename(columns={"Date": "tr_date"}, inplace=True)
    lotsize_df["tr_date"] = pd.to_datetime(lotsize_df["tr_date"]).dt.strftime(
        "%d-%m-%Y"
    )
    lotsize_df.rename(columns={"BankNifty": "Lot_Size"}, inplace=True)
    fnoieddf["tr_time"] = fnoieddf["tr_time"].astype(str)
    fnoieddf["tr_date"] = pd.to_datetime(fnoieddf["tr_date"]).dt.strftime("%d-%m-%Y")
    # Getting stoploss target values from config
    stoploss_target_combo = json.loads(TARGET_STOPLOSS_VALUES)
    stoploss_value = stoploss_target_combo[1][0]
    target_value = stoploss_target_combo[0][1]

    # Step 1 : Finding Spot close value
    find_spot_close = bnifty_df[bnifty_df["tr_time"] == ENTRY_TIME]
    find_spot_close["spot_price"] = find_spot_close.apply(
        lambda row: get_spot_close_price(row, ENTRY_TIME, bnifty_df), axis=1
    )

    # Step 3 grouping the date and otype  and applying to each row of
    # fnoieddf df and resetting indexes.
    find_close_price = (
        fnoieddf.groupby(["tr_date", "otype"])
        .apply(
            lambda group: get_closest_strike_price(
                stoploss_value, target_value, group, ENTRY_TIME, CLOSEST_VAL
            )
        )
        .reset_index(drop=True)
    )
    # merging spot close price onto the dataframe
    find_exit_conditions = find_close_price.merge(
        find_spot_close[["tr_date", "spot_price"]], on="tr_date"
    )

    # Step 4 EXIT CONDITION
    find_exit_conditions[
        ["exit_type", "exit_price", "exit_time"]
    ] = find_exit_conditions.apply(
        lambda row: apply_exit_conditions(
            row["tr_date"],
            row["otype"],
            row["target"],
            row["stoploss"],
            ENTRY_TIME,
            SQUAREOFF_TIME,
            find_close_price,
        ),
        axis=1,
        result_type="expand",
    )
    # Step 5: Add lot size column and prepare final DataFrame
    find_exit_conditions = add_lotsize_column(lotsize_df, find_exit_conditions)
    # Step 6: Adding Profit and Loss Column
    find_exit_conditions["PNL"] = (
        find_exit_conditions["entry_price"] - find_exit_conditions["exit_price"]
    ) * find_exit_conditions["lotsize"]
    columns_to_drop = [
        "tr_open",
        "tr_high",
        "tr_low",
        "tr_close",
        "expiry_date",
        "month_expiry",
        "tr_segment",
        "week_expiry",
    ]
    find_exit_conditions = find_exit_conditions.drop(
        columns=columns_to_drop, errors="ignore"
    )
    print(find_exit_conditions)

    output_csv_path = "sc0002_v1.csv"
    find_exit_conditions.to_csv(output_csv_path, index=False)


if __name__ == "__main__":
    # Read the config file
    config = configparser.ConfigParser()
    config.read("config.ini")
    ENTRY_TIME = str(config.get("params", "entry_time"))
    WEEK_EXPIRY = int(config.get("params", "week_expiry"))
    SQUAREOFF_TIME = str(config.get("params", "squareoff_time"))
    TR_SEGMENT = int(config.get("params", "tr_segment"))
    TARGET_STOPLOSS_VALUES = config.get("params", "stoploss_target_combo")
    CLOSEST_VAL = int(config.get("params", "closest_val"))

    fnoieddf = pd.read_excel("FNO_DATA.xlsx")
    fnoieddf = fnoieddf[
        (fnoieddf["week_expiry"] == WEEK_EXPIRY)
        & (fnoieddf["tr_segment"] == TR_SEGMENT)
    ]
    main()
