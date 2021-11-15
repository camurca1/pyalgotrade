# https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/
# https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-a-vista/codigo-isin/pesquisa/
# https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/dados-publicos-de-produtos-listados-e-de-balcao/

import pandas as pd


# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)

isin_instruments = pd.read_csv('dados/isinp/NUMERACA.TXT',
                               sep=',',
                               header=None,
                               engine='python')
isin_instruments = pd.DataFrame(isin_instruments.values[:, 2:6],
                                columns=['ISIN', 'cod_emissor', 'prefix_emissor', 'desc_ativo'])
isin_instruments['cod_emissor'] = isin_instruments['cod_emissor'].astype(str)
isin_emissores = pd.read_csv('dados/isinp/EMISSOR.TXT',
                             sep=',',
                             header=None,
                             engine='python')

isin_emissores = pd.DataFrame(isin_emissores.values[:, 0:3],
                                columns=['cod_emissor', 'emissor', 'CNPJ'])
isin_emissores['cod_emissor'] = isin_emissores['cod_emissor'].astype(str)
isin_emissores['CNPJ'] = isin_emissores['CNPJ'].astype(str).str.replace('.0', '', regex=False)

isin_instruments = pd.merge(isin_instruments, isin_emissores, on=['cod_emissor']).drop_duplicates()
cad = pd.read_csv('dados/SPW_CIA_ABERTA.txt',
                  sep='\t',
                  encoding='WINDOWS-1252')
cad = cad[['CD_CVM', 'DENOM_SOCIAL', 'DENOM_COMERC',
          'SETOR_ATIV', 'CNPJ', 'DT_REG', 'DT_CONST',
          'DT_CANCEL', 'MOTIVO_CANCEL', 'SIT_REG', 'SIT_EMISSOR']]
cad['CNPJ'] = cad['CNPJ'].astype(str)

instrumentos = pd.read_csv('dados/InstrumentsConsolidatedFile_20211101_1.csv',
                           sep=';',
                           encoding='WINDOWS-1252',
                           low_memory=False)
instrumentos = instrumentos[instrumentos['SctyCtgyNm'] == 'SHARES']

df_empresas = instrumentos[['TckrSymb', 'Asst', 'SgmtNm', 'MktNm', 'SctyCtgyNm',
                            'ISIN', 'SpcfctnCd', 'CrpnNm', 'CorpGovnLvlNm']]
del instrumentos

df_empresas = pd.merge(df_empresas, isin_instruments, on=['ISIN']).drop_duplicates()
df_empresas = pd.merge(df_empresas, cad, on=['CNPJ']).drop_duplicates()
df_empresas.to_csv('dados/empresas_monitoradas.csv', index=False)
