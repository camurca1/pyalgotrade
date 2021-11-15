# %%
import pandas as pd
import numpy as np
import yfinance as yf
from pandas_datareader import data as web
import matplotlib.pyplot as plt

# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 2000000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: '%.9f' % x)
pd.set_option('use_inf_as_na', True)

# %%
empresas = pd.read_csv('dados/fundamentos/dfp_cias_monitoradas_BPP_con_2010-2021.csv')
tickers = pd.read_csv('dados/empresas_monitoradas.csv')
tickers = tickers[tickers['SgmtNm'] != 'ODD LOT']
tickers = tickers[['CD_CVM', 'TckrSymb']]

# %%
# passo 1 - eliminar empresas que apresentaram PL negativo e sem historico
patrimonio = empresas[empresas['DS_CONTA'].str.contains('Patrimônio Líquido Consolidado')]
patrimonio = patrimonio[['CD_CVM', 'DT_REFER', 'DENOM_CIA', 'ESCALA_MOEDA', 'CD_CONTA', 'DS_CONTA', 'VL_CONTA']]
patrimonio['DT_REFER'] = pd.to_datetime(patrimonio['DT_REFER'])
patrimonio['VL_CONTA'] = patrimonio['VL_CONTA'].astype(float)
patrimonio = patrimonio[patrimonio['VL_CONTA'] > 0]
patrimonio['VL_CONTA'] = np.where(patrimonio['ESCALA_MOEDA'] == 'MIL',
                                  patrimonio['VL_CONTA'] * 1000,
                                  patrimonio['VL_CONTA'])
obs_completas = patrimonio.groupby(['CD_CVM']).size().reset_index(name='counts')
obs_completas = obs_completas[obs_completas['counts'] == 10]
patrimonio = patrimonio[patrimonio['CD_CVM'].isin(obs_completas['CD_CVM'])]

del obs_completas
del empresas
print('Passo 1 concluído.')
# %%
# passo 2 - liquidez diaria maior que R$ 500 mil
lista_empresas = pd.merge(patrimonio['CD_CVM'], tickers,
                          on=['CD_CVM'], how='left').drop_duplicates().reset_index(drop=True)

tickers = lista_empresas['TckrSymb'].tolist()
volume = pd.DataFrame(columns=['TckrSymb', 'volume_medio'])
for ticker in tickers:
    try:
        precos = pd.read_csv(f'dados/cotacoes/{ticker}_daily.csv', parse_dates=True, index_col=['datetime'])
        precos = precos[precos.index > '2015-12-31']
        precos['volume'] = precos['close'] * precos['real_volume']
        volume_medio = precos['volume'].mean()
        # print(f'O volume médio diário de {ticker} é R$ {volume_medio:.2f}.')
        if volume_medio > 500000:
            volume = volume.append({'TckrSymb': ticker, 'volume_medio': volume_medio}, ignore_index=True)
    except:
        # print(f'O ativo {ticker} não é negociado.')
        pass

lista_empresas = lista_empresas[lista_empresas['TckrSymb'].isin(volume['TckrSymb'])]
lista_empresas = pd.merge(lista_empresas, volume,
                          on=['TckrSymb'], how='left').drop_duplicates().reset_index(drop=True)

del volume
del volume_medio
del tickers

print('Passo 2 concluído.')

# %%
# passo 3 - retirar bancos da analise
bancos = pd.read_csv('dados/empresas_monitoradas.csv')
setores = bancos[['CD_CVM', 'SETOR_ATIV']].drop_duplicates()
del bancos

lista_empresas = pd.merge(lista_empresas, setores, on=['CD_CVM'], how='left')
lista_empresas = lista_empresas[~lista_empresas['SETOR_ATIV'].str.contains('Bancos')]
lista_empresas = lista_empresas[~lista_empresas['SETOR_ATIV'].str.contains('Financeira')]
lista_empresas.reset_index(drop=True)
del setores

print('Passo 3 concluído.')

# %%
# passo 4 - manter a classe com mais liquidez de cada ativo
lista_empresas = lista_empresas.groupby('CD_CVM').max().sort_values(by='TckrSymb').reset_index()

# Valor de mercado
tickers = lista_empresas['TckrSymb'].tolist()

market_cap = pd.read_csv('dados/fundamentos/fre_cias_monitoradas_distribuicao_capital_2010-2020.csv')
market_cap = market_cap[['CD_CVM', 'DT_REFER', 'Quantidade_Total_Acoes_Circulacao']]
market_cap = market_cap[market_cap['CD_CVM'].isin(lista_empresas['CD_CVM'])]
market_cap = market_cap.groupby(['CD_CVM', 'DT_REFER']).sum().reset_index()
market_cap = pd.merge(market_cap, lista_empresas[['CD_CVM', 'TckrSymb']], on=['CD_CVM'], how='left')
market_cap['DT_REFER'] = pd.to_datetime(market_cap['DT_REFER'])
market_cap = pd.merge(market_cap, patrimonio[['CD_CVM', 'DT_REFER', 'VL_CONTA']], on=['CD_CVM', 'DT_REFER'], how='left')

ticker_market_cap = pd.DataFrame()

for ticker in tickers:
    precos = pd.read_csv(f'dados/cotacoes/{ticker}_daily.csv', parse_dates=True)
    precos.rename({'datetime': 'DT_REFER'}, axis=1, inplace=True)
    precos['DT_REFER'] = pd.to_datetime(precos['DT_REFER'])
    precos = precos[precos['DT_REFER'] > '2009-12-30']
    precos = precos[['DT_REFER', 'close']]
    precos.set_index('DT_REFER', inplace=True)
    market_cap_aux = market_cap[market_cap['TckrSymb'] == ticker]
    market_cap_aux.set_index('DT_REFER', inplace=True)
    market_cap_aux = precos.join(market_cap_aux, how='outer')
    market_cap_aux.fillna(method='ffill', inplace=True)
    market_cap_aux.dropna(inplace=True)
    market_cap_aux['CD_CVM'] = market_cap_aux['CD_CVM'].astype(int)
    market_cap_aux['valormercado'] = market_cap_aux['close'] * market_cap_aux['Quantidade_Total_Acoes_Circulacao']
    market_cap_aux.reset_index(inplace=True)
    ticker_market_cap = pd.concat([ticker_market_cap, market_cap_aux])

ticker_market_cap.set_index(['TckrSymb', 'CD_CVM', 'DT_REFER'], inplace=True)
ticker_market_cap.rename({'VL_CONTA': 'valorpatrimonio'}, axis=1, inplace=True)

del market_cap_aux
del tickers

print('Passo 4 concluído.')

# %%
# Calculo BE/ME
mediana = ticker_market_cap['valormercado'].groupby('DT_REFER').median().reset_index().rename(
    columns={'valormercado': 'valormedianadia'})
mediana.set_index('DT_REFER', inplace=True)
index_list = ticker_market_cap.index.get_level_values('TckrSymb').drop_duplicates().tolist()
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    mediana, on='DT_REFER', how='left')
ticker_market_cap['BE/ME'] = ticker_market_cap['valorpatrimonio'] / ticker_market_cap['valormercado']

del mediana

print('BE/ME calculados.')

# %%
# Definicao BIG e SMALL
ticker_market_cap['ME'] = np.where(ticker_market_cap['valormercado'] > ticker_market_cap['valormedianadia'], 'B', 'S')

 # %%
# Definicao Low, Middle, High
percentil_30 = ticker_market_cap['BE/ME'].groupby('DT_REFER').quantile(.3).reset_index().rename(
    columns={'BE/ME': 'percentil_30'})
percentil_30.set_index('DT_REFER', inplace=True)
percentil_70 = ticker_market_cap['BE/ME'].groupby('DT_REFER').quantile(.7).reset_index().rename(
    columns={'BE/ME': 'percentil_70'})
percentil_70.set_index('DT_REFER', inplace=True)
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    percentil_30, on='DT_REFER', how='left')
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    percentil_70, on='DT_REFER', how='left')
ticker_market_cap['L/M/H'] = np.where(ticker_market_cap['BE/ME'] < ticker_market_cap['percentil_30'], 'L', 'M')
ticker_market_cap['L/M/H'] = np.where(ticker_market_cap['BE/ME'] > ticker_market_cap['percentil_70'], 'H',
                                      ticker_market_cap['L/M/H'])
ticker_market_cap['cod_portifolio'] = ticker_market_cap['ME'] + '/' + ticker_market_cap['L/M/H']

del percentil_70
del percentil_30

# %%
# Calculo do ROE
dre = pd.read_csv('dados/fundamentos/dfp_cias_monitoradas_DRE_con_2010-2021.csv')
lucro_liquido = dre[
    dre['DS_CONTA'].str.contains('Lucro/Prejuízo Consolidado do Período') | dre['DS_CONTA'].str.contains(
        'Lucro ou Prejuízo Líquido Consolidado do Período')]
lucro_liquido = lucro_liquido[['CD_CVM', 'DT_REFER', 'DENOM_CIA', 'ESCALA_MOEDA', 'CD_CONTA', 'DS_CONTA', 'VL_CONTA']]
lucro_liquido['DT_REFER'] = pd.to_datetime(lucro_liquido['DT_REFER'])
lucro_liquido['VL_CONTA'] = lucro_liquido['VL_CONTA'].astype(float)
lucro_liquido['VL_CONTA'] = np.where(lucro_liquido['ESCALA_MOEDA'] == 'MIL',
                                     lucro_liquido['VL_CONTA'] * 1000,
                                     lucro_liquido['VL_CONTA'])

ticker_market_cap = ticker_market_cap.reset_index().merge(lucro_liquido[['CD_CVM', 'DT_REFER', 'VL_CONTA']],
                                                          on=['CD_CVM', 'DT_REFER'],
                                                          how='left').set_index(['TckrSymb', 'CD_CVM', 'DT_REFER'])

ticker_market_cap.fillna(method='ffill', inplace=True)
ticker_market_cap.rename({'VL_CONTA': 'lucroliquido'}, axis=1, inplace=True)
ticker_market_cap['ROE'] = ticker_market_cap['lucroliquido'] / ticker_market_cap['valorpatrimonio']

del dre
del lucro_liquido

print('ROE calculado.')

# %%
# Definicao Rentabilidade - RMW
percentil_30 = ticker_market_cap['ROE'].groupby('DT_REFER').quantile(.3).reset_index().rename(
    columns={'ROE': 'ROE_percentil_30'})
percentil_30.set_index('DT_REFER', inplace=True)
percentil_70 = ticker_market_cap['ROE'].groupby('DT_REFER').quantile(.7).reset_index().rename(
    columns={'ROE': 'ROE_percentil_70'})
percentil_70.set_index('DT_REFER', inplace=True)
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    percentil_30, on='DT_REFER', how='left')
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    percentil_70, on='DT_REFER', how='left')
ticker_market_cap['Rentabilidade'] = np.where(ticker_market_cap['ROE'] < ticker_market_cap['ROE_percentil_30'], 'W',
                                              'M')
ticker_market_cap['Rentabilidade'] = np.where(ticker_market_cap['ROE'] > ticker_market_cap['ROE_percentil_70'], 'R',
                                              ticker_market_cap['Rentabilidade'])
ticker_market_cap['RMW'] = ticker_market_cap['ME'] + '/' + ticker_market_cap['Rentabilidade']

del percentil_30
del percentil_70

# %%
# Calculo da variacao do Ativo
ativo_total = pd.read_csv('dados/fundamentos/dfp_cias_monitoradas_BPA_con_2010-2021.csv')
ativo_total = ativo_total[ativo_total['DS_CONTA'].str.contains('Ativo Total')]
ativo_total = ativo_total[['CD_CVM', 'DT_REFER', 'DENOM_CIA', 'ESCALA_MOEDA', 'CD_CONTA', 'DS_CONTA', 'VL_CONTA']]
ativo_total['DT_REFER'] = pd.to_datetime(ativo_total['DT_REFER'])
ativo_total['VL_CONTA'] = ativo_total['VL_CONTA'].astype(float)
ativo_total['VL_CONTA'] = np.where(ativo_total['ESCALA_MOEDA'] == 'MIL',
                                   ativo_total['VL_CONTA'] * 1000,
                                   ativo_total['VL_CONTA'])
ativo_total['pct_change'] = ativo_total.groupby(['CD_CVM'])['VL_CONTA'].pct_change().fillna(0)
ticker_market_cap = ticker_market_cap.reset_index().merge(ativo_total[['CD_CVM', 'DT_REFER', 'pct_change']],
                                                          on=['CD_CVM', 'DT_REFER'],
                                                          how='left').set_index(['TckrSymb', 'CD_CVM', 'DT_REFER'])

ticker_market_cap.fillna(method='ffill', inplace=True)
ticker_market_cap.rename({'pct_change': 'ativopctchange'}, axis=1, inplace=True)

del ativo_total

print('Variacao do ativo calculado.')

# %%
# Definicao de Investimentos - CMA
percentil_30 = ticker_market_cap['ativopctchange'].groupby('DT_REFER').quantile(.3).reset_index().rename(
    columns={'ativopctchange': 'ATchange_percentil_30'})
percentil_30.set_index('DT_REFER', inplace=True)
percentil_70 = ticker_market_cap['ativopctchange'].groupby('DT_REFER').quantile(.7).reset_index().rename(
    columns={'ativopctchange': 'ATchange_percentil_70'})
percentil_70.set_index('DT_REFER', inplace=True)
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    percentil_30, on='DT_REFER', how='left')
ticker_market_cap = ticker_market_cap[ticker_market_cap.index.get_level_values('TckrSymb').isin(index_list)].join(
    percentil_70, on='DT_REFER', how='left')
ticker_market_cap['Investimento'] = np.where(
    ticker_market_cap['ativopctchange'] < ticker_market_cap['ATchange_percentil_30'], 'C', 'M')
ticker_market_cap['Investimento'] = np.where(
    ticker_market_cap['ativopctchange'] > ticker_market_cap['ATchange_percentil_70'], 'A',
    ticker_market_cap['Investimento'])
ticker_market_cap['CMA'] = ticker_market_cap['ME'] + '/' + ticker_market_cap['Investimento']

del percentil_30
del percentil_70

# %%
# Carteiras indice Book-To_Market
empresas_dez = ticker_market_cap[(ticker_market_cap.index.get_level_values('DT_REFER').month == 12) & (
            ticker_market_cap.index.get_level_values('DT_REFER').day == 31)]
# %%
empresas_dez = empresas_dez.reset_index().groupby(['DT_REFER', 'cod_portifolio'])['TckrSymb'].agg(
    lambda x: list(x)).reset_index().rename(columns={'TckrSymb': 'acoes'})
carteiras_sl_ano = empresas_dez[empresas_dez['cod_portifolio'] == 'S/L'].reset_index(drop=True)
carteiras_sm_ano = empresas_dez[empresas_dez['cod_portifolio'] == 'S/M'].reset_index(drop=True)
carteiras_sh_ano = empresas_dez[empresas_dez['cod_portifolio'] == 'S/H'].reset_index(drop=True)
carteiras_bl_ano = empresas_dez[empresas_dez['cod_portifolio'] == 'B/L'].reset_index(drop=True)
carteiras_bm_ano = empresas_dez[empresas_dez['cod_portifolio'] == 'B/M'].reset_index(drop=True)
carteiras_bh_ano = empresas_dez[empresas_dez['cod_portifolio'] == 'B/H'].reset_index(drop=True)

carteiras_bm = [carteiras_sl_ano, carteiras_sm_ano, carteiras_sh_ano, carteiras_bl_ano, carteiras_bm_ano, carteiras_bh_ano]

# %%
# Calcular retornos médios das carteiras be/me

ano = 2011
ret_carteira = pd.DataFrame(columns=['DT_REFER'])
retorno_medio = pd.DataFrame(columns=['Ano', 'Tamanho', 'Retorno', 'Acoes'])
for tamanho in carteiras_bm:
    for carteira in tamanho.loc[:, 'acoes']:
        for ticker in carteira:
            precos = pd.read_csv(f'dados/cotacoes/{ticker}_daily.csv', parse_dates=True)
            precos.rename({'datetime': 'DT_REFER', 'close': f'{ticker}'}, axis=1, inplace=True)
            precos['DT_REFER'] = pd.to_datetime(precos['DT_REFER'])
            precos = precos[(precos['DT_REFER'] >= f'{ano}-01-01') & (precos['DT_REFER'] <= f'{ano}-12-31')]
            precos = precos[['DT_REFER', f'{ticker}']]
            precos.set_index('DT_REFER', inplace=True)
            ret_carteira['DT_REFER'] = pd.date_range(start=f'{ano}-01-01', end=f'{ano}-12-31', freq='B')
            ret_carteira['DT_REFER'] = pd.to_datetime(ret_carteira['DT_REFER'])
            ret_carteira.set_index('DT_REFER', inplace=True)
            ret_carteira = ret_carteira.join(precos, how='left')
            ret_carteira.fillna(method='ffill', inplace=True)
            ret_carteira.fillna(0, inplace=True)
            # pesos = [(1 / len(carteira)) for acao in carteira]
            acoes = carteira
        ret_carteira = ret_carteira.pct_change(fill_method='ffill')[3:]
        # ret_carteira = (pesos * ret_carteira)
        ret_carteira = ret_carteira.sum(axis=1)
        retorno_medio = retorno_medio.append({'Ano': ano, 'Tamanho': tamanho.loc[0, 'cod_portifolio'],
                                              'Retorno': f'{ret_carteira.mean():.6f}',
                                              'Acoes': acoes},
                                             ignore_index=True)
        retorno_medio.reset_index(drop=True, inplace=True)
        ret_carteira = pd.DataFrame(columns=['DT_REFER'])
        ano += 1
    ano = 2011

print('Retornos das carteiras be/me calculados.')

# %%
retorno_medio[['BigSmall', 'Crescimento']] = retorno_medio['Tamanho'].str.split('/', 1, expand=True)
retorno_medio['Retorno'] = retorno_medio['Retorno'].astype(float)
retorno_medio_smb = retorno_medio.groupby(['Ano', 'BigSmall'])['Retorno'].mean().reset_index().rename(columns={'TckrSymb': 'acoes'})

# %%
SMB_bm = retorno_medio_smb.pivot_table('Retorno', ['BigSmall'], 'Ano')
SMB_bm = SMB_bm.diff().dropna().reset_index(drop=True)

# %%
retorno_medio_hml = retorno_medio[~retorno_medio['Crescimento'].str.contains('M')]
retorno_medio_hml = retorno_medio_hml.groupby(['Ano', 'Crescimento'])['Retorno'].mean().reset_index().rename(columns={'TckrSymb': 'acoes'})
HML = retorno_medio_hml.pivot_table('Retorno', ['Crescimento'], 'Ano')
HML = HML.diff().dropna().reset_index(drop=True)

# %%
# Carteiras Rentabilidade RMW
empresas_dez = ticker_market_cap[(ticker_market_cap.index.get_level_values('DT_REFER').month == 12) & (
            ticker_market_cap.index.get_level_values('DT_REFER').day == 31)]
empresas_dez = empresas_dez.reset_index().groupby(['DT_REFER', 'RMW'])['TckrSymb'].agg(
    lambda x: list(x)).reset_index().rename(columns={'TckrSymb': 'acoes'})
carteiras_sw_ano = empresas_dez[empresas_dez['RMW'] == 'S/W'].reset_index(drop=True)
carteiras_sn_ano = empresas_dez[empresas_dez['RMW'] == 'S/M'].reset_index(drop=True)
carteiras_sr_ano = empresas_dez[empresas_dez['RMW'] == 'S/R'].reset_index(drop=True)
carteiras_bw_ano = empresas_dez[empresas_dez['RMW'] == 'B/W'].reset_index(drop=True)
carteiras_bn_ano = empresas_dez[empresas_dez['RMW'] == 'B/M'].reset_index(drop=True)
carteiras_br_ano = empresas_dez[empresas_dez['RMW'] == 'B/R'].reset_index(drop=True)

carteiras_ren = [carteiras_sw_ano, carteiras_sn_ano, carteiras_sr_ano, carteiras_bw_ano, carteiras_bn_ano, carteiras_br_ano]

# %%
# Calcular retornos médios das carteiras rmw
ano = 2011
ret_carteira = pd.DataFrame(columns=['DT_REFER'])
retorno_medio = pd.DataFrame(columns=['Ano', 'Tamanho', 'Retorno', 'Acoes'])
for tamanho in carteiras_ren:
    for carteira in tamanho.loc[:, 'acoes']:
        for ticker in carteira:
            precos = pd.read_csv(f'dados/cotacoes/{ticker}_daily.csv', parse_dates=True)
            precos.rename({'datetime': 'DT_REFER', 'close': f'{ticker}'}, axis=1, inplace=True)
            precos['DT_REFER'] = pd.to_datetime(precos['DT_REFER'])
            precos = precos[(precos['DT_REFER'] >= f'{ano}-01-01') & (precos['DT_REFER'] <= f'{ano}-12-31')]
            precos = precos[['DT_REFER', f'{ticker}']]
            precos.set_index('DT_REFER', inplace=True)
            ret_carteira['DT_REFER'] = pd.date_range(start=f'{ano}-01-01', end=f'{ano}-12-31', freq='B')
            ret_carteira['DT_REFER'] = pd.to_datetime(ret_carteira['DT_REFER'])
            ret_carteira.set_index('DT_REFER', inplace=True)
            ret_carteira = ret_carteira.join(precos, how='left')
            ret_carteira.fillna(method='ffill', inplace=True)
            ret_carteira.fillna(0, inplace=True)
            # pesos = [(1 / len(carteira)) for acao in carteira]
            acoes = carteira
        ret_carteira = ret_carteira.pct_change(fill_method='ffill')[3:]
        # ret_carteira = (pesos * ret_carteira)
        ret_carteira = ret_carteira.sum(axis=1)
        retorno_medio = retorno_medio.append({'Ano': ano, 'Tamanho': tamanho.loc[0, 'RMW'],
                                              'Retorno': f'{ret_carteira.mean():.6f}',
                                              'Acoes': acoes},
                                             ignore_index=True)
        retorno_medio.reset_index(drop=True, inplace=True)
        ret_carteira = pd.DataFrame(columns=['DT_REFER'])
        ano += 1
    ano = 2011

print('Retornos das carteiras rmw calculados.')

# %%
retorno_medio[['BigSmall', 'Crescimento']] = retorno_medio['Tamanho'].str.split('/', 1, expand=True)
retorno_medio['Retorno'] = retorno_medio['Retorno'].astype(float)
retorno_medio_ren = retorno_medio.groupby(['Ano', 'BigSmall'])['Retorno'].mean().reset_index().rename(columns={'TckrSymb': 'acoes'})

# %%
SMB_ren = retorno_medio_ren.pivot_table('Retorno', ['BigSmall'], 'Ano')
SMB_ren = SMB_ren.diff().dropna().reset_index(drop=True)

# %%
retorno_medio_rmw = retorno_medio[~retorno_medio['Crescimento'].str.contains('M')]
retorno_medio_rmw = retorno_medio_rmw.groupby(['Ano', 'Crescimento'])['Retorno'].mean().reset_index().rename(columns={'TckrSymb': 'acoes'})
RMW = retorno_medio_rmw.pivot_table('Retorno', ['Crescimento'], 'Ano')
RMW = RMW.diff(-1).dropna().reset_index(drop=True)

# %%
# Carteiras Investimento CMA
empresas_dez = ticker_market_cap[(ticker_market_cap.index.get_level_values('DT_REFER').month == 12) & (
            ticker_market_cap.index.get_level_values('DT_REFER').day == 31)]
empresas_dez = empresas_dez.reset_index().groupby(['DT_REFER', 'CMA'])['TckrSymb'].agg(
    lambda x: list(x)).reset_index().rename(columns={'TckrSymb': 'acoes'})
carteiras_sc_ano = empresas_dez[empresas_dez['CMA'] == 'S/C'].reset_index(drop=True)
carteiras_sni_ano = empresas_dez[empresas_dez['CMA'] == 'S/M'].reset_index(drop=True)
carteiras_sa_ano = empresas_dez[empresas_dez['CMA'] == 'S/A'].reset_index(drop=True)
carteiras_bc_ano = empresas_dez[empresas_dez['CMA'] == 'B/C'].reset_index(drop=True)
carteiras_bni_ano = empresas_dez[empresas_dez['CMA'] == 'B/M'].reset_index(drop=True)
carteiras_ba_ano = empresas_dez[empresas_dez['CMA'] == 'B/A'].reset_index(drop=True)

carteiras_inv = [carteiras_sc_ano, carteiras_sni_ano, carteiras_sa_ano, carteiras_bc_ano, carteiras_bni_ano, carteiras_ba_ano]


# %%
# Calcular retornos médios das carteiras cma
ano = 2012
ret_carteira = pd.DataFrame(columns=['DT_REFER'])
retorno_medio = pd.DataFrame(columns=['Ano', 'Tamanho', 'Retorno', 'Acoes'])
for tamanho in carteiras_inv:
    for carteira in tamanho.loc[:, 'acoes']:
        for ticker in carteira:
            precos = pd.read_csv(f'dados/cotacoes/{ticker}_daily.csv', parse_dates=True)
            precos.rename({'datetime': 'DT_REFER', 'close': f'{ticker}'}, axis=1, inplace=True)
            precos['DT_REFER'] = pd.to_datetime(precos['DT_REFER'])
            precos = precos[(precos['DT_REFER'] >= f'{ano}-01-01') & (precos['DT_REFER'] <= f'{ano}-12-31')]
            precos = precos[['DT_REFER', f'{ticker}']]
            precos.set_index('DT_REFER', inplace=True)
            ret_carteira['DT_REFER'] = pd.date_range(start=f'{ano}-01-01', end=f'{ano}-12-31', freq='B')
            ret_carteira['DT_REFER'] = pd.to_datetime(ret_carteira['DT_REFER'])
            ret_carteira.set_index('DT_REFER', inplace=True)
            ret_carteira = ret_carteira.join(precos, how='left')
            ret_carteira.fillna(method='ffill', inplace=True)
            ret_carteira.fillna(0, inplace=True)
            # pesos = [(1 / len(carteira)) for acao in carteira]
            acoes = carteira
        ret_carteira = ret_carteira.pct_change(fill_method='ffill')[3:]
        # ret_carteira = (pesos * ret_carteira)
        ret_carteira = ret_carteira.sum(axis=1)
        retorno_medio = retorno_medio.append({'Ano': ano, 'Tamanho': tamanho.loc[0, 'CMA'],
                                              'Retorno': f'{ret_carteira.mean():.6f}',
                                              'Acoes': acoes},
                                             ignore_index=True)
        retorno_medio.reset_index(drop=True, inplace=True)
        ret_carteira = pd.DataFrame(columns=['DT_REFER'])
        ano += 1
    ano = 2012

print('Retornos das carteiras cma calculados.')

# %%
retorno_medio[['BigSmall', 'Crescimento']] = retorno_medio['Tamanho'].str.split('/', 1, expand=True)
retorno_medio['Retorno'] = retorno_medio['Retorno'].astype(float)
retorno_medio_inv = retorno_medio.groupby(['Ano', 'BigSmall'])['Retorno'].mean().reset_index().rename(columns={'TckrSymb': 'acoes'})

# %%
SMB_inv = retorno_medio_inv.pivot_table('Retorno', ['BigSmall'], 'Ano')
SMB_inv = SMB_inv.diff().dropna().reset_index(drop=True)

# %%
retorno_medio_cma = retorno_medio[~retorno_medio['Crescimento'].str.contains('M')]
retorno_medio_cma = retorno_medio_cma.groupby(['Ano', 'Crescimento'])['Retorno'].mean().reset_index().rename(columns={'TckrSymb': 'acoes'})
CMA = retorno_medio_cma.pivot_table('Retorno', ['Crescimento'], 'Ano')
CMA = CMA.diff().dropna().reset_index(drop=True)

# %%
SMB = (SMB_bm + SMB_ren + SMB_inv)/3
SMB.iloc[0, 0] = (SMB_bm.iloc[0, 0] + SMB_ren.iloc[0, 0])/2

# %%
fatores = pd.DataFrame(columns=SMB.columns)
fatores = fatores.append(pd.DataFrame([SMB.loc[0].tolist()], index=['SMB'], columns=SMB.columns))
fatores = fatores.append(pd.DataFrame([HML.loc[0].tolist()], index=['HML'], columns=HML.columns))
fatores = fatores.append(pd.DataFrame([RMW.loc[0].tolist()], index=['RMW'], columns=RMW.columns))
fatores = fatores.append(pd.DataFrame([CMA.loc[0].tolist()], index=['CMA'], columns=CMA.columns))
fatores = fatores.T
fatores.reset_index(inplace=True)
fatores['Ano'] = fatores['Ano'].astype(str) + '-12-31'
fatores['Ano'] = pd.to_datetime(fatores['Ano'])
fatores.set_index('Ano', inplace=True)
fatores.drop(index='2021-12-31', inplace=True)
fatores.fillna(0, inplace=True)

# %%
ibov = web.DataReader('^BVSP', 'yahoo', '01-01-2011', '31-12-2020')
ibov = ibov['Close'].reset_index().rename(columns={'Date': 'DT_REFER', 'Close': 'close'}).set_index('DT_REFER')
ibov['ret_ibov'] = ibov['close'].pct_change()
ibov['ret_ibov'].plot()

# %%
url = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json'
cdi = pd.read_json(url)
cdi['data'] = pd.to_datetime(cdi['data'], dayfirst=True)
cdi = cdi.reset_index(drop=True).rename(columns={'data': 'DT_REFER', 'valor': 'ret_cdi'}).set_index('DT_REFER')
cdi = cdi[cdi.index >= '2011-01-01']

# %%
mktrf = pd.DataFrame()
mktrf['DT_REFER'] = pd.date_range(start='2011-01-01', end='2020-12-31')
mktrf['DT_REFER'] = pd.to_datetime(mktrf['DT_REFER'])
mktrf.set_index('DT_REFER', inplace=True)
mktrf = pd.merge(mktrf, ibov['ret_ibov'], on='DT_REFER', how='left')
mktrf = pd.merge(mktrf, cdi['ret_cdi'], on='DT_REFER', how='left')
mktrf.fillna(method='ffill', inplace=True)
mktrf['Mkt-RF'] = mktrf['ret_ibov'] - mktrf['ret_cdi']


 # %%
fatores = fatores.reset_index().rename(columns={'Ano': 'DT_REFER'}).set_index('DT_REFER')
fatores = pd.merge(fatores, mktrf['Mkt-RF'], on='DT_REFER', how='left')
fatores = fatores[['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']]

print(fatores)
print('Tabela de fatores concluida.')

# %%
fatores.to_csv('dados/fama_factors.csv', index=True)
ticker_market_cap.to_csv('dados/mkt_cap.csv', index=True)

