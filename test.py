import pandas as pd
from datetime import time

bnifty_df = pd.read_csv('spot_data.csv')
#print(bnifty_df.head())
dup_df = bnifty_df.copy()
dup_df['tr_time'] = dup_df['tr_time'].astype(str)
df2 = dup_df[dup_df['tr_time'] == '09:18:59' ]

duplicated_df = pd.concat([df2, df2], ignore_index=True)
alternating_otype = ['CE', 'PE'] * (len(duplicated_df) // 2 + 1)
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

merged_df['target']=merged_df['entry_price']*0.5
merged_df['stoploss']=merged_df['entry_price']*1.5



#Step 4
#target rule

merged_df = merged_df.drop(columns=['tr_segment', 'month_expiry', 'week_expiry'])
print(merged_df)
print("------------------Step4----------------------------------")
#filter from fnodata ,fr now single data, later fr 3dates and different svals
# df1 = dff[dff['tr_date'] == '2019-01-01']
# print(df1)
# df2 = df1[df1['week_expiry'] == 1]
# sval=27100
# df3=df2[df2['strike_price']==sval]
# df4=df3[df3['otype']=='CE']
merged_dfss = []
def proces_new(date, sval):
    df1 = dff[dff['tr_date'] == date]
    df3 = df1[df1['week_expiry'] == 1]
    df4 = df3[df3['otype'].isin(['CE', 'PE'])]
    df5 = df4[df4['strike_price']== sval]
    df5['entry_price'] = df5['tr_close']
    return df5
dates = ['2019-01-01', '2019-01-02', '2019-01-03']
svals = [27100,27200,27100]

for date, sval in zip(dates, svals):
    merged_dfss.append(proces_new(date, sval))

if merged_dfss:
    merged_dfk = pd.concat(merged_dfss, ignore_index=True)
else:
    print("No data found.")
print(merged_dfk)

# columns = ['exit_time', 'exit_price', 'exit_type']
# df5 = pd.DataFrame(columns=columns)
# # Assuming you've already loaded your CSV file into a DataFrame named 'dff'
# df5['exit_time'] = ''
# df5['exit_price'] = ''
# df5['exit_type'] = ''

# # Iterate through the DataFrame rows
# for index, row in df4.iterrows():
#     target = row['entry_price'] / 2
#     stoploss = row['entry_price'] * 1.5
#     # Check if it's before '15:19:59'

#     if row['tr_time'] <= '15:19:59':
#         # Check target exit condition
#         if row['tr_time'] > '09:18:59' and row['tr_low'] <= target:
#             df5.at[index, 'exit_time'] = row['tr_time']
#             df5.at[index, 'exit_price'] = 102.9
#             df5.at[index, 'exit_type'] = 'TARGET'
#             break
        
#         # Check stoploss exit condition
#         if row['tr_time'] > '09:18:59' and row['tr_high'] >= stoploss:
#             df5.at[index, 'exit_time'] = row['tr_time']
#             df5.at[index, 'exit_price'] = 308.7
#             df5.at[index, 'exit_type'] = 'STOPLOSS'
#             break
        
#         # Check if it's '15:19:59'
#         if row['tr_time'] == '15:19:59':
#             df5.at[index, 'exit_time'] = '15:19:59'
#             df5.at[index, 'exit_price'] = row['tr_close']
#             df5.at[index, 'exit_type'] = 'SQOFF'
#             break

# print(df5)

output_csv_path = 'df4.csv'
merged_dfk.to_csv(output_csv_path, index=False)






