import pandas as pd


pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: '%.9f' % x)
pd.set_option('use_inf_as_na', True)


def concateDates(df):
    df['date'] = df['year'].astype(str) + '-' \
                 + df['month'].astype(str) + '-' \
                 + df['day'].astype(str)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    columns = ['year', 'month', 'day']
    df.drop(columns, axis=1, inplace=True)
    return df

# %%
factors = ['Market', 'SMB', 'HML', 'WML', 'IML']
factor_df = pd.DataFrame()
for factor in factors:
    url = f'http://www.nefin.com.br/Risk%20Factors/{factor}_Factor.xls'
    factor_hist = pd.read_excel(url)
    factor_hist = concateDates(factor_hist)
    factor_df = pd.concat([factor_df, factor_hist], axis=1)
factor_df.rename(columns={'Rm_minus_Rf': 'RmRf'}, inplace=True)
factor_df.index.name = 'DT_REFER'

# %%
factor_df.to_csv('dados/fama_factors.csv')