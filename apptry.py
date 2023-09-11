import pandas as pd
from datetime import time

bnifty_df = pd.read_csv('spot_data.csv')
dup_df = bnifty_df.copy()
dup_df['tr_time'] = dup_df['tr_time'].astype(str)
df2 = dup_df[dup_df['tr_time'] == '09:18:59' ]

print(df2)

duplicated_df = pd.concat([df2, df2], ignore_index=True)
print(duplicated_df)
alternating_otype = ['CE', 'PE'] * (len(duplicated_df) // 2 + 1)
print(alternating_otype)
duplicated_df['otype'] = alternating_otype[:len(duplicated_df)]
duplicated_df['strike_price'] = (duplicated_df['tr_close'] // 100) * 100
print(duplicated_df)

#based on the values of strike price and otype in step 2 we r using it in step3
#Step 3
excel_file_path = 'FNO_DATA.xlsx'  # Replace with your file path
dff = pd.read_excel(excel_file_path)
dup_df = dff.copy()




merged_dfs = []
def process_data(date, sval):
    df1 = dup_df[dup_df['tr_date'] == date]
    df1['tr_time'] = df1['tr_time'].astype(str)
    df2 = df1[df1['tr_time'] == '09:18:59']
    df3 = df2[df2['week_expiry'] == 1]
    df4 = df3[df3['otype'].isin(['CE', 'PE'])]
    df5 = df4[df4['strike_price']== sval]
    df5['entry_price'] = df5['tr_close']
    return df5

dates = ['2019-01-01', '2019-01-02', '2019-01-03']
svals = [27100, 27200,27100]

for date, sval in zip(dates, svals):
    merged_dfs.append(process_data(date, sval))

if merged_dfs:
    merged_df = pd.concat(merged_dfs, ignore_index=True)
else:
    print("No data found.")
print(merged_df)
print("-----------")
merged_df['target']=merged_df['entry_price']*0.5
merged_df['stoploss']=merged_df['entry_price']*1.5

merged_df = merged_df.drop(columns=['tr_segment', 'month_expiry', 'week_expiry'])
print(merged_df)
#Step 4
#target rule
excel_file_path = 'FNO_DATA.xlsx'  # Replace with your file path
dff = pd.read_excel(excel_file_path)
dff['tr_time'] = dff['tr_time'].astype(str)

exit_conditions = [
    {'tr_date':'2019-01-01','otype': 'CE','strike_price':27100, 'low_condition': 102.9, 'high_condition': 308.7},
    {'tr_date':'2019-01-01','otype': 'PE','strike_price':27100,'low_condition': 61.025, 'high_condition': 183.075},
    {'tr_date':'2019-01-02','otype': 'CE','strike_price':27200, 'low_condition': 86.725, 'high_condition': 260.175},
    {'tr_date':'2019-01-02','otype': 'PE','strike_price':27200, 'low_condition': 41.925, 'high_condition': 125.775},
    {'tr_date':'2019-01-03','otype': 'CE','strike_price':27100, 'low_condition': 52.525, 'high_condition': 157.575},
    {'tr_date':'2019-01-03','otype': 'PE','strike_price':27100, 'low_condition': 32.625, 'high_condition': 97.875}
]

columns = ['exit_time', 'exit_price', 'exit_type','tr_date','otype']
df5 = pd.DataFrame(columns=columns)
# Assuming you've already loaded your CSV file into a DataFrame named 'dff'
df5['exit_time'] = ''
df5['exit_price'] = ''
df5['exit_type'] = ''
df5['tr_date'] = ''
df5['otype'] = ''

for condition in exit_conditions:
    subset_df = dff.query(
    "tr_date == @condition['tr_date'] and "
    "week_expiry == 1 and "
    "strike_price == @condition['strike_price'] and "
    "otype == @condition['otype']"
)
    print(subset_df)
    df_list = []
    for index, row in subset_df.iterrows():
        exit_price = None
        exit_type = None

        if row['tr_time'] <= '15:19:59':
            # Check target exit condition
            if row['tr_time'] > '09:18:59' and row['tr_low'] <= condition['low_condition']:
                df5.at[index, 'exit_time'] = row['tr_time']
                df5.at[index, 'exit_price'] = condition['low_condition']
                df5.at[index, 'exit_type'] = 'TARGET'
                df5['tr_date'] = df5[ 'tr_date'].astype(str)
                df5.at[index, 'otype'] = condition['otype']
                df5.at[index, 'tr_date'] = condition['tr_date']
                break
            
            # Check stoploss exit condition
            if row['tr_time'] > '09:18:59' and row['tr_high'] >= condition['high_condition']:
                df5.at[index, 'exit_time'] = row['tr_time']
                df5.at[index, 'exit_price'] = condition['high_condition']
                df5.at[index, 'exit_type'] = 'STOPLOSS'
                df5['tr_date'] = df5[ 'tr_date'].astype(str)
                df5.at[index, 'otype'] = condition['otype']
                df5.at[index, 'tr_date'] = condition['tr_date']
                break
            
            # Check if it's '15:19:59'
            if row['tr_time'] == '15:19:59':
                df5.at[index, 'exit_time'] = '15:19:59'
                df5.at[index, 'exit_price'] = row['tr_close']
                df5.at[index, 'exit_type'] = 'SQOFF'
                df5['tr_date'] = df5[ 'tr_date'].astype(str)
                df5.at[index, 'otype'] = condition['otype']
                df5.at[index, 'tr_date'] = condition['tr_date']
                break
    df5 = df5.append(df5, ignore_index=True)
    df5 = df5.drop_duplicates()

df5['tr_date'] = df5[ 'tr_date'].astype(str)
merged_df['tr_date'] = merged_df[ 'tr_date'].astype(str)
dfstep4 = pd.merge(merged_df,df5, on=['otype', 'tr_date'],how='inner')

print("---------------Step 5----------------------------")

lotsize_df = pd.read_csv('LotSize_Data.csv')
new_column_name = 'tr_date'
lotsize_df.rename(columns={'Date': new_column_name}, inplace=True)
lotsize_df['tr_date'] = lotsize_df['tr_date'].astype(str)
dflot = lotsize_df[lotsize_df['tr_date'].isin(['01-01-2019', '02-01-2019', '03-01-2019'])]
print(dflot)

dflot['tr_date']= pd.to_datetime(dflot['tr_date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
dflot['tr_date'] = dflot['tr_date'].astype(str)
dfstep4['tr_date'] = dfstep4['tr_date'].astype(str)
dfplot = pd.merge(dfstep4,dflot, on='tr_date')
dfplot = dfplot.drop('Nifty', axis=1)
dfplot.rename(columns={'BankNifty': 'Lot_Size'}, inplace=True)
print(dfplot)

columns_to_drop = ['tr_open', 'tr_high','tr_low','week_expiry_date','expiry_date']
dfplot = dfplot.drop(columns=columns_to_drop, errors='ignore')
output_csv_path = 'dfplot.csv'
dfplot.to_csv(output_csv_path, index=False)