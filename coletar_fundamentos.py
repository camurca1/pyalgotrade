import pandas as pd
import wget
from zipfile import ZipFile

# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)

arquivos_zip = []
df = pd.read_csv('dados/empresas_monitoradas.csv')
hist_files_path = 'dados/fundamentos/'
nomes = ['BPA_con', 'BPA_ind',
         'BPP_con', 'BPP_ind',
         'DFC_MD_con', 'DFC_MD_ind',
         'DFC_MI_con', 'DFC_MI_ind',
         'DMPL_con', 'DMPL_ind',
         'DRA_con', 'DRA_ind',
         'DRE_con', 'DRE_ind',
         'DVA_con', 'DVA_ind']


def get_cias_ativas(cad, atualizar=False):
    cad = cad[cad['SIT_REG'] != 'CANCELADA']
    cad = cad[cad['SIT_REG'] != 'SUSPENSO(A) - DECISÃO ADM']
    cad = cad[cad['SIT_EMISSOR'] != 'LIQUIDAÇÃO EXTRAJUDICIAL']
    cad = cad[cad['SIT_EMISSOR'] != 'PARALISADA']
    cad = cad[cad['SIT_EMISSOR'] != 'EM RECUPERAÇÃO JUDICIAL OU EQUIVALENTE']
    cad = cad[cad['SIT_EMISSOR'] != 'EM LIQUIDAÇÃO JUDICIAL']
    cad = cad[cad['SIT_EMISSOR'] != 'FALIDA']
    cad = cad[cad['SIT_EMISSOR'] != 'EM RECUPERAÇÃO EXTRAJUDICIAL']
    cad = cad.drop_duplicates()
    cad['CD_CVM'] = cad['CD_CVM'].astype(int)
    return cad


def get_fund_data_itr(ano_inicio, ano_fim, atualizar=False):
    url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'
    save_path = 'dados/ITR_forms/'
    extract_path = 'dados/ITR_forms/forms/'
    if atualizar:

        for ano in range(ano_inicio, ano_fim + 1):
            arquivos_zip.append(f'itr_cia_aberta_{ano}.zip')

        for arquivo in arquivos_zip:
            url = f'{url_base}{arquivo}'
            wget.download(url, save_path)
            print(f'{arquivo} salvo em {save_path}.')
            ZipFile(save_path + arquivo, 'r').extractall(extract_path)
            print(f'{arquivo} extraído em {extract_path}.')

        for nome in nomes:
            arquivo = pd.DataFrame()
            for ano in range(ano_inicio, ano_fim + 1):
                arquivo = pd.concat([arquivo, pd.read_csv(f'{extract_path}itr_cia_aberta_{nome}_{ano}.csv',
                                                          sep=';',
                                                          decimal=',',
                                                          encoding='ISO-8859-1')])
            arquivo.to_csv(f'{hist_files_path}itr_cia_aberta_{nome}_2011-2021.csv', index=False)
            print(f'Histórico de {nome} salvo em {hist_files_path}.')


def get_fund_data_dfp(ano_inicio, ano_fim, atualizar=False):
    url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
    save_path = 'dados/DFP_forms/'
    extract_path = 'dados/DFP_forms/forms/'
    if atualizar:

        for ano in range(ano_inicio, ano_fim + 1):
            arquivos_zip.append(f'dfp_cia_aberta_{ano}.zip')

        for arquivo in arquivos_zip:
            url = f'{url_base}{arquivo}'
            wget.download(url, save_path)
            print(f'{arquivo} salvo em {save_path}.')
            ZipFile(save_path + arquivo, 'r').extractall(extract_path)
            print(f'{arquivo} extraído em {extract_path}.')


def get_dfp_monitored_symbols(empresas, atualizar=False):
    if atualizar:
        for nome in nomes:
            dfp_empresas_monitoradas = pd.DataFrame()
            for empresa in empresas:
                dfp = pd.read_csv(f'{hist_files_path}dfp_cia_aberta_{nome}_2010-2021.csv')

                dfp = dfp[dfp['ORDEM_EXERC'] == 'ÚLTIMO']
                dfp_empresas_monitoradas = pd.concat([dfp_empresas_monitoradas,
                                                     dfp[dfp['CD_CVM'] == empresa]])
            dfp_empresas_monitoradas.to_csv(f'{hist_files_path}dfp_cias_monitoradas_{nome}_2010-2021.csv', index=False)
            print(f'Histórico de {nome} salvo em {hist_files_path}.')


if __name__ == '__main__':
    get_fund_data_itr(2011, 2021, atualizar=False)
    get_fund_data_dfp(2010, 2021, atualizar=False)
    df = get_cias_ativas(df, True)
    cod_cvm = df['CD_CVM'].drop_duplicates().to_list()
    get_dfp_monitored_symbols(cod_cvm, atualizar=False)
