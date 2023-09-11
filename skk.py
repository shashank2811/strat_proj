# import pandas as pd

# # Load your CSV data into a DataFrame (assuming you have done this already)
# df = pd.read_excel('FNO_DATA.xlsx',header=0)

# # Set the column names manually
# dup_df = df.copy()
# print(dup_df.info())
# # print(df)
# # date = 
# # sval = 
# # otype = '
# # df_filtered = df[(df['tr_date'] == '2019-01-01')]
# # print(df_filtered)

# # df1 = dff[dff['tr_date'] == '2019-01-01']
# # # print(df1)
# # df1['tr_time'] = df1['tr_time'].astype(str)
# # df2 = df1[df1['tr_time'] == tr_tym ]
# # df3 = df2[df2['week_expiry'] == 1]
# # df4=df3[df3['otype']=='CE']
# # df5=df4[df4['strike_price']==sval]

# # ?print(df5)     
# merged_dfs = []
# def process_data(date, sval):
#     df1 = dup_df[dup_df['tr_date'] == date]
#     print(df1)  # Print intermediate result
#     df1['tr_time'] = df1['tr_time'].astype(str)
#     df2 = df1[df1['tr_time'] == '09:18:59']
#     print(df2)  # Print intermediate result
#     df3 = df2[df2['week_expiry'] == 1]
#     print("Step 3:", df3)  # Print intermediate result
#     df4 = df3[df3['otype'] == 'PE']
#     print("Step 4:", df4)  # Print intermediate result
#     df5 = df4[df4['strike_price'] == sval]
#     print("Step 5:", df5)  # Print intermediate result
#     df5['entry_price'] = df5['tr_close']
#     return df5

# dates = ['2019-01-01', '2019-01-02', '2019-01-03']
# svals = [27100, 27200,27100]

# for date, sval in zip(dates, svals):
#     merged_dfs.append(process_data(date, sval))

# if merged_dfs:
#     merged_df = pd.concat(merged_dfs, ignore_index=False)
#     print(merged_df)
# else:
#     print("No data found.")

# # df1 = dff[dff['tr_date'] == '2019-01-01']
# # # print(df1)

# # df1['tr_time'] = df1['tr_time'].astype(str)
# # df2 = df1[df1['tr_time'] == '09:18:59' ]
# # sval=27100
# # df3=df2[df2['strike_price']==sval]
# # df4=df3[df3['otype']=='CE']
# # df13 = df4[df4['week_expiry'] == 1]
# # df13['entry_price']=df13['tr_close']


# # df5 = dff[dff['tr_date'] == '2019-01-02']
# # df5['tr_time'] = df5['tr_time'].astype(str)
# # df6 = df5[df5['tr_time'] == '09:18:59' ]
# # sval=27200
# # df7=df6[df6['strike_price']==sval]
# # df8=df7[df7['otype']=='CE']
# # df14 = df8[df8['week_expiry'] == 1]
# # df14['entry_price']=df14['tr_close']

# # df9 = dff[dff['tr_date'] == '2019-01-03']
# # df9['tr_time'] = df9['tr_time'].astype(str)
# # df10 = df9[df9['tr_time'] == '09:18:59' ]
# # sval=27100
# # df11=df10[df10['strike_price']==sval]
# # df12=df11[df11['otype']=='CE']
# # df15 = df12[df12['week_expiry'] == 1]
# # df15['entry_price']=df15['tr_close']

# # merged_df = pd.concat([df13, df14, df15], ignore_index=True)
# # df13 = merged_df[merged_df['week_expiry'] == 1] 
#the entire thing si wrongggg
import pandas as pd

excel_file_path = 'FNO_DATA.xlsx'  # Replace with your file path
dff = pd.read_excel(excel_file_path)
dup_df = dff.copy()
merged_dfs = []

def process_data(date):
    df1 = dup_df[dup_df['tr_date'] == date]
    df1['tr_time'] = df1['tr_time'].astype(str)
    df2 = df1[df1['tr_time'] > '09:18:59']
    df3 = df2[df2['week_expiry'] == 1]
    df4 = df3[df3['otype']=='CE']
    df5 = df4[df4['strike_price']==27100]
    df5['entry_price'] = df5['tr_close']
    return df5

dates = ['2019-01-01']

for date in dates:
    merged_dfs.append(process_data(date))

if merged_dfs:
    merged_df = pd.concat(merged_dfs, ignore_index=True)
    merged_df = merged_df.drop(columns=['tr_segment', 'month_expiry', 'week_expiry'])
else:
    print("No data found.")
merged_df['tr_time'] = merged_df['tr_time'].astype(str)
merged_df['target'] = merged_df['entry_price'] * 0.5
merged_df['stoploss'] = merged_df['entry_price'] * 1.5


# Assuming you have your DataFrame named merged_df

# Initialize columns for exit information
merged_df['exit_time'] = ''
merged_df['exit_price'] = ''
merged_df['exit_type'] = ''

# Iterate through the DataFrame rows
for index, row in merged_df.iterrows():
    # Check if it's before '15:19:59'
    if row['tr_time'] <= '15:19:59':
        # Check target exit condition
        if row['tr_time'] > '09:18:59' and row['tr_low'] <= 102.9:
            merged_df.at[index, 'exit_time'] = row['tr_time']
            merged_df.at[index, 'exit_price'] = 102.9
            merged_df.at[index, 'exit_type'] = 'TARGET'
            break
        
        # Check stoploss exit condition
        if row['tr_time'] > '09:18:59' and row['tr_high'] >= 308.7:
            merged_df.at[index, 'exit_time'] = row['tr_time']
            merged_df.at[index, 'exit_price'] = 308.7
            merged_df.at[index, 'exit_type'] = 'STOPLOSS'
            break
        
        # Check if it's '15:19:59'
        if row['tr_time'] == '15:19:59':
            merged_df.at[index, 'exit_time'] = '15:19:59'
            merged_df.at[index, 'exit_price'] = row['tr_close']
            merged_df.at[index, 'exit_type'] = 'SQOFF'
            break

print(merged_df)

# Apply conditions and create the exit columns
# merged_df['targetexit'] = (merged_df['tr_low'] <= 102.9) & (merged_df['tr_time'] > '09:18:59')
# merged_df['stoplossexit'] = (merged_df['tr_high'] >= 308.7) & (merged_df['tr_time'] > '09:18:59')

# # Initialize exit columns


# # Update exit columns based on conditions
# for index, row in merged_df.iterrows():
#     if row['targetexit']:
#         merged_df.at[index, 'exit_time'] = row['tr_time']
#         merged_df.at[index, 'exit_price'] = 102.9
#         merged_df.at[index, 'exit_type'] = 'TARGET'
#         break
#     elif row['stoplossexit']:
#         merged_df.at[index, 'exit_time'] = row['tr_time']
#         merged_df.at[index, 'exit_price'] = 308.7
#         merged_df.at[index, 'exit_type'] = 'STOPLOSS'
#         break
#     else:
#         merged_df['exit_time'] = '15:19:59'
#         merged_df['exit_price'] = merged_df['tr_close']
#         merged_df['exit_type'] = 'SQOFF'
#         break

# print(merged_df)

# output_csv_path = 'stockyydata.csv'
# merged_df.to_csv(output_csv_path, index=False)

### Final use
# exit_time = []
# exit_price = []
# exit_type = ''

# # df4['tr_time'] = df4['tr_time'].astype(str)
# for index, row in merged_dfk.iterrows():
#     # Calculate target and stoploss values
#     target = row['entry_price'] / 2
#     stoploss = row['entry_price'] * 1.5
    
#     # Check if it's before '15:19:59'
#     if row['tr_time'] <= '15:19:59':
#         # Check target exit condition
#         if row['tr_time'] > '09:18:59' and row['tr_low'] <= target:
#             exit_time = row['tr_time']
#             exit_price = target
#             exit_type = 'TARGET'
#             break
        
#         # Check stoploss exit condition
#         if row['tr_time'] > '09:18:59' and row['tr_high'] >= stoploss:
#             exit_time = row['tr_time']
#             exit_price = stoploss
#             exit_type = 'STOPLOSS'
#             break
        
#         # Check if it's '15:19:59'
#         if row['tr_time'] == '15:19:59':
#             exit_time = '15:19:59'
#             exit_price = row['tr_close']
#             exit_type = 'SQOFF'
#             break
# final_result_df = pd.DataFrame(columns=['tr_date', 'tr_time', 'tr_open', 'tr_high', 'tr_low', 'tr_close', 'week_expiry_date', 'strike_price', 'otype', 'entry_price', 'target', 'stoploss','exit_time','exit_price','exit_type'])
# # Append exit-related information to final_result_df
# final_result_df = final_result_df.append({
#     'tr_date': row['tr_date'],
#     'tr_time': row['tr_time'],
#     'tr_open': row['tr_open'],
#     'tr_high': row['tr_high'],
#     'tr_low': row['tr_low'],
#     'tr_close': row['tr_close'],
#     'week_expiry_date': row['week_expiry_date'],
#     'strike_price': row['strike_price'],
#     'otype': row['otype'],
#     'entry_price': row['entry_price'],
#     'exit_type': exit_type,
#     'target': exit_price if exit_type == 'TARGET' else None,
#     'stoploss': exit_price if exit_type == 'STOPLOSS' else None,
#     'exit_time': exit_time,
#     'exit_price': exit_price
# }, ignore_index=True)

# print(final_result_df)
