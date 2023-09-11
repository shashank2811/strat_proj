import pandas as pd
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
# dup_df['tr_time'] = dup_df['tr_time'].astype(str)
# dftrtime=dup_df['tr_time'] 
# dflow=merged_df['tr_low']
# dfhigh=merged_df['tr_high']
# dfotype=merged_df['otype']
# dfstrprice=merged_df['strike_price']
# dfstploss=merged_df['stoploss']
# dftarget=merged_df['target']
# if((dftrtime>'09:18:59') & (dflow<=dftarget)):
#     print("Exit Time",dftrtime)
print("------------------Step4----------------------------------")
#filter from fnodata ,fr now single data, later fr 3dates and different svals
df1 = dff[dff['tr_date'] == '2019-01-01']
# print(df1)
df2 = df1[df1['week_expiry'] == 1]
sval=27100
df3=df2[df2['strike_price']==sval]
df4=df3[df3['otype']=='CE']
print(df4)



# output_csv_path = 'stocknew_data.csv'
# merged_df.to_csv(output_csv_path, index=False)






