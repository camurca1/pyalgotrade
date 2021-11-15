import pandas as pd
from pylab import mpl, plt


# configurar exibição pandas
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)


ativo = 'ITUB4'
csv_path = f'dados/{ativo}_daily.csv'

df_full = pd.read_csv(csv_path, parse_dates=True, index_col=['datetime'])

df = pd.DataFrame(df_full['close'])
df['SMA1'] = df['close'].rolling(42).mean()
df['SMA2'] = df['close'].rolling(252).mean()

plt.style.use('seaborn')
mpl.rcParams['savefig.dpi'] = 300
mpl.rcParams['font.family'] = 'serif'

df.plot(title='ITUB4 | 42 & 252 days SMAs', figsize=(10, 6))
