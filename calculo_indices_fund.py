import pandas as pd
import matplotlib.pyplot as plt


# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)

precos = pd.read_csv('dados/cotacoes/ITSA4_daily.csv', parse_dates=True)
precos.rename({'datetime': 'DT_REFER'}, axis=1, inplace=True)
precos = precos[precos['DT_REFER'] > '2012-01-01']
precos['DT_REFER'] = pd.to_datetime(precos['DT_REFER'])
precos.set_index('DT_REFER', inplace=True)
precos = precos[['close']]
empresas = pd.read_csv('dados/fundamentos/dfp_cias_monitoradas_DRE_con_2010-2021.csv')


empresa = empresas[empresas['CD_CVM'] == 7617]

lista_contas = empresas[['CD_CONTA', 'DS_CONTA']].drop_duplicates().set_index('CD_CONTA')
conta = empresa[empresa['CD_CONTA'] == '3.99.01.02']
conta.index = pd.to_datetime(conta['DT_REFER'])

indicadores = precos.join(conta['VL_CONTA'], how='outer')
indicadores.rename({'VL_CONTA': 'LPA'}, axis=1, inplace=True)
indicadores.fillna(method='ffill', inplace=True)
indicadores.dropna(inplace=True)
indicadores['PL'] = indicadores['close'] / indicadores['LPA']

plt.plot(indicadores.index, indicadores['PL'])
plt.plot(indicadores.index, indicadores['close'])
plt.title('P/L ITSA4 2012 - 2021')
plt.xlabel('Anos')
plt.savefig('figs/pl_itsa4_historico.png')


