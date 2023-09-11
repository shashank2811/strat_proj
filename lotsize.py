import pandas as pd

lotsize_df = pd.read_csv('LotSize_Data.csv')
new_column_name = 'tr_date'
lotsize_df.rename(columns={'Date': new_column_name}, inplace=True)
lotsize_df['tr_date'] = lotsize_df['tr_date'].astype(str)
dflot = lotsize_df[lotsize_df['tr_date'].isin(['01-01-2019', '02-01-2019', '03-01-2019'])]
print(dflot)


# Convert date format
dflot['tr_date']= pd.to_datetime(dflot['tr_date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
print(dflot)