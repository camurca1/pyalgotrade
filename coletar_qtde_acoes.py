import pandas as pd
import wget
from zipfile import ZipFile
import re

# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 2000000)
pd.set_option('display.expand_frame_repr', False)

arquivos_zip = []
df = pd.read_csv('dados/empresas_monitoradas.csv')
hist_files_path = 'dados/fundamentos/'
anos = [ano for ano in range(2010, 2022)]


def get_fund_data_fre(ano_inicio, ano_fim, atualizar=False):
    url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/'
    save_path = 'dados/FRE_forms/'
    extract_path = 'dados/FRE_forms/forms/'
    if atualizar:

        for ano in range(ano_inicio, ano_fim + 1):
            arquivos_zip.append(f'fre_cia_aberta_{ano}.zip')

        for arquivo in arquivos_zip:
            url = f'{url_base}{arquivo}'
            wget.download(url, save_path)
            print(f'{arquivo} salvo em {save_path}.')
            ZipFile(save_path + arquivo, 'r').extractall(extract_path)
            print(f'{arquivo} extraído em {extract_path}.')


df['CNPJ'] = df['CNPJ'].astype(str).str.zfill(14)
empresas = df['CNPJ'].tolist()

fre_monitoradas = pd.DataFrame()

for ano in anos:
    fre = pd.read_csv(f'dados/FRE_forms/forms/fre_cia_aberta_distribuicao_capital_{ano}.csv',
                      sep=';',
                      encoding='WINDOWS-1252')

    chars_to_remove = ['.', '-', '/']
    regular_expression = '[' + re.escape(''.join(chars_to_remove)) + ']'
    fre['CNPJ_Companhia'] = fre['CNPJ_Companhia'].str.replace(regular_expression, '', regex=True)
    fre = fre[fre['CNPJ_Companhia'].isin(empresas)].reset_index(drop=True)
    fre['Data_Referencia'] = pd.to_datetime(fre['Data_Referencia']) - pd.to_timedelta(1, unit='d')
    fre_monitoradas = pd.concat([fre_monitoradas, fre])


fre_monitoradas.rename({'CNPJ_Companhia': 'CNPJ'}, axis=1, inplace=True)
fre_monitoradas.rename({'Data_Referencia': 'DT_REFER'}, axis=1, inplace=True)
fre_monitoradas = pd.merge(fre_monitoradas, df[['CNPJ', 'CD_CVM']], on=['CNPJ'], how='left')
fre_monitoradas.reset_index(drop=True, inplace=True)
fre_monitoradas.to_csv('dados/fundamentos/fre_cias_monitoradas_distribuicao_capital_2010-2020.csv', index=False)
