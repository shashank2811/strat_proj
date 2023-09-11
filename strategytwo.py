import pandas as pd
import configparser

def filter_spot_data(bnifty_df, entry_time):
    return bnifty_df[bnifty_df['tr_time'] == entry_time]

def filter_fno_data(dff, entry_time, week_expiry):
    filtered_dff = dff.loc[
        (dff['tr_time'] == entry_time) & (dff['week_expiry'] == week_expiry)
    ]
    
    filtered_dff = (
        filtered_dff.groupby(['tr_date', 'week_expiry', 'otype'])
        .apply(lambda group: group[group['tr_close'] >= 200].nsmallest(1, 'tr_close'))
        .reset_index(drop=True)
    )
    
    filtered_dff['entry_price'] = filtered_dff['tr_close']
    filtered_dff = filtered_dff.assign(
        target=filtered_dff['entry_price'] * 0.5,
        stoploss=filtered_dff['entry_price'] * 1.5
    )
    
    return filtered_dff

def apply_exit_conditions(row):
    exit_type = None
    exit_price = None
    exit_time = None
    
    tr_time = row['tr_time']
    entry_time = row['entry_time']
    target = row['target']
    stoploss = row['stoploss']
    
    if tr_time > entry_time:
        if row['tr_low'] <= target:
            exit_type = 'TARGET'
            exit_price = target
            exit_time = tr_time
        elif row['tr_high'] >= stoploss:
            exit_type = 'STOPLOSS'
            exit_price = stoploss
            exit_time = tr_time
    elif tr_time == squareoff_time:
        exit_type = 'SQOFF'
        exit_price = row['tr_close']
        exit_time = squareoff_time
    
    row['exit_type'] = exit_type
    row['exit_price'] = exit_price
    row['exit_time'] = exit_time
    
    return row

def main():
    
    lotsize_df.rename(columns={'Date': 'tr_date', 'BankNifty': 'Lot_Size'}, inplace=True)
    lotsize_df['tr_date'] = pd.to_datetime(lotsize_df['tr_date']).dt.strftime('%d-%m-%Y')
    
    dfspot = filter_spot_data(bnifty_df, entry_time)
    print(dfspot)
    
    df_fno = filter_fno_data(dff, entry_time, week_expiry)
    filterdf = df_fno.apply(apply_exit_conditions, axis=1)
    
    output_csv_path = 'FilterDf.csv'
    filterdf.to_csv(output_csv_path, index=False)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    entry_time = str(config.get('params', 'entry_time'))
    week_expiry = int(config.get('params', 'week_expiry'))
    squareoff_time= str(config.get('params', 'squareoff_time'))
    excel_file_path = 'FNO_DATA.xlsx'
    dff = pd.read_excel(excel_file_path)
    bnifty_df = pd.read_csv('spot_data.csv')
    bnifty_df['tr_time'] = pd.to_datetime(bnifty_df['tr_time']).dt.time
    # dff['tr_time'] = pd.to_datetime(dff['tr_time']).dt.time
    lotsize_df = pd.read_csv('LotSize_Data.csv')
    main()
