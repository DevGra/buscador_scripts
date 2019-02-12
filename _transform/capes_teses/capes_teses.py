# coding=utf8
import pandas as pd
import os
from settings import BASE_PATH_DATA
import csv
import commands
import datetime
import errno
import re
from utils.utils import *


# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

class CapesTeses(object):
    """
    A classe CapesTeses é responsável pela transformaçao da base de dados(Collection - capes_teses),
    faz parte do processo de ETL(Extração, Transformação e Carga).

    Atributos:
        date            (date): data de execução deste arquivo.
        input_lenght    (int): Variável que irá guardar a quantidade de linhas do arquivo de entrada(download).
        output_length   (int): Variável que irá guardar a quantidade de linhas do arquivo de saída(transform).
    """

    def __init__(self, year):
        """
        Construtor da classe CapesTeses, recebe 1 parâmetro.

        PARAMETRO:
            year   (str): Nome do diretório da pasta download, em: BASE_PATH_DATA + 'capes_teses/year/download/'.

        """
        self.date = datetime.datetime.now()
        self.ano = year
        self.input_lenght = 0
        self.output_length = 0

    def pega_arquivo_ano(self):
        '''
        Este método itera os arquivos em: BASE_PATH_DATA + capes_teses/ano/download,
        trata os nomes dos diretórios e conta as linhas dos arquivos.

        PARAMETRO:
            Não recebe parâmetro.

        RETORNO:
            Retorna um dataframe.

        '''
        var = BASE_PATH_DATA + 'capes_teses/' + str(self.ano) + '/download/'
        exclude_prefixes = ('__', '.')
        for root, dirs, files in os.walk(var, topdown=True):
            dirs[:] = [dirname for dirname in dirs if not dirname.startswith(exclude_prefixes)]
            for f in files:
                if f.endswith('.csv'):
                    arquivo = open(os.path.join(root, f), 'r')
                    self.input_lenght = commands.getstatusoutput('cat ' + os.path.join(root, f) + ' |wc -l')[1]
                    print 'Arquivo {} de entrada possui {} linhas de informacao'.format(f, int(self.input_lenght) - 1)
                    df = pd.read_csv(arquivo, sep=';', low_memory=False, engine='c', encoding='latin1', )

                    return df

    def resolve_dicionarios(self):
        """
        Método para modificar/alterar/atualizar/remover colunas e linhas
        do dataframe e também resolver o(s) dicionário(s), pega o retorno
        do método pega_arquivo_ano, corrige o formato das datas,
        preenche os campos inteiros vazios do dataframe com 0, resolve os campos para facet,
        busca e nuvem de palavras, faz os ajustes dos campos do dataframe.

        PARAMETRO:
            Não recebe parâmetro.

        RETORNO:
            Retorna um dataframe pronto para ser convertido em csv pelo método gera_csv.

        """
        df = self.pega_arquivo_ano()
        parse_dates = ['DT_MATRICULA', 'DH_INICIO_AREA_CONC', 'DH_FIM_AREA_CONC',
                       'DH_INICIO_LINHA', 'DH_FIM_LINHA', 'DT_TITULACAO']

        for dt in parse_dates:
            if self.ano == '2017':
                df[dt] = pd.to_datetime(df[dt], infer_datetime_format=False, format='%d%b%Y %H:%M:%S', errors='coerce')
            else:
                df[dt] = pd.to_datetime(df[dt], infer_datetime_format=False, format='%d%b%Y:%H:%M:%S', errors='coerce')

        df['AN_BASE'] = df['AN_BASE'].fillna(self.ano).astype(int)
        #df['ID_ADD_PRODUCAO_INTELECTUAL'] = df['ID_ADD_PRODUCAO_INTELECTUAL'].fillna(0).astype(int)
        #df['ID_PRODUCAO_INTELECTUAL'] = df['ID_PRODUCAO_INTELECTUAL'].fillna(0).astype(int)
        #df['ID_SUBTIPO_PRODUCAO'] = df['ID_SUBTIPO_PRODUCAO'].fillna(0).astype(int)
        #df['IN_ORIENT_PARTICIPOU_BANCA'] = df['IN_ORIENT_PARTICIPOU_BANCA'].fillna(0).astype(int)
        #df['ID_PESSOA_DISCENTE'] = df['ID_PESSOA_DISCENTE'].fillna(0).astype(int)
        #df['ID_GRAU_ACADEMICO'] = df['ID_GRAU_ACADEMICO'].fillna(0).astype(int)
        #df['CD_GRANDE_AREA_CONHECIMENTO'] = df['CD_GRANDE_AREA_CONHECIMENTO'].fillna(0).astype(int)
        #df['CD_AREA_CONHECIMENTO'] = df['CD_AREA_CONHECIMENTO'].fillna(0).astype(int)
        df['NR_PAGINAS'] = df['NR_PAGINAS'].fillna(0).astype(int)

        df['AN_BASE_facet'] = gYear(self.ano)
        df['ANO_MATRICULA_facet'] = df[df['DT_MATRICULA'].notnull()]['DT_MATRICULA'].dt.year.apply(gYear)
        #df['ANO_MATRICULA_facet'] = df[df['DT_MATRICULA'].dt.year == '2013']['DT_MATRICULA'].dt.year.apply(gYear)
        df['ANO_TITULACAO_facet'] = df['DT_TITULACAO'].dt.year.apply(gYear)
        df['ANO_INICIO_LINHA_facet'] = df['DH_INICIO_LINHA'].dt.year.apply(gYear)
        df['ANO_FIM_LINHA_facet'] = df['DH_FIM_LINHA'].dt.year.apply(gYear)
        df['GEOGRAFICO_IES_facet'] = df['NM_REGIAO'] + '|' + df['NM_UF_IES'] + '-' + df['SG_UF_IES']
        df['AREA_CONHECIMENTO_facet'] = df['NM_GRANDE_AREA_CONHECIMENTO'] + '|' + df[
            'NM_AREA_CONHECIMENTO']

        df['DS_PALAVRA_CHAVE_exact'] = df['DS_PALAVRA_CHAVE'].apply(norm_keyword)
        df['DS_KEYWORD_exact'] = df['DS_KEYWORD'].apply(norm_keyword)
        df['NM_ORIENTADOR_exact'] = df['NM_ORIENTADOR']
        df['NM_DISCENTE_exact'] = df['NM_DISCENTE']
        df['NM_PROJETO_exact'] = df['NM_PROJETO']
        df['NM_PROGRAMA_exact'] = df['NM_PROGRAMA']
        df['NM_AREA_CONCENTRACAO_exact'] = df['NM_AREA_CONCENTRACAO']
        df['NM_LINHA_PESQUISA_exact'] = df['NM_LINHA_PESQUISA']
        df['NM_ENTIDADE_ENSINO_exact'] = df['NM_ENTIDADE_ENSINO']
        df['NM_AREA_AVALIACAO_exact'] = df['NM_AREA_AVALIACAO'].apply(norm_palavra_avaliacao)
        df['NM_AREA_CONHECIMENTO'] = df['NM_AREA_CONHECIMENTO'].apply(norm_keyword).upper()

        df['TITULO_RESUMO'] = df['NM_PRODUCAO'] + '\n' + df['DS_RESUMO']

        df['DT_MATRICULA'] = df[dt].dt.strftime('%Y%m%d')
        df['DT_MATRICULA'] = df['DT_MATRICULA'].apply(data_facet).astype(str)
        df['DT_TITULACAO'] = df[dt].dt.strftime('%Y%m%d')
        df['DT_TITULACAO'] = df['DT_TITULACAO'].apply(data_facet).astype(str)
        #import pdb; pdb.set_trace()
        return df

    def gera_csv(self):
        """
        Método recebe o retorno do método resolve_dicionario,
        cria os arquivos de saída(.csv e .log) e o diretório de destino,
        conta as linhas do arquivo .csv e os grava no diretório de destino.

        PARAMETRO:
            Não recebe parâmetro.

        RETORNO:
            Método sem retorno, mostra apenas uma mensagem de processamento finalizado.

        """
        df = self.resolve_dicionarios()
        destino_transform = BASE_PATH_DATA + 'capes_teses/' + str(self.ano) + '/transform'
        csv_file = '/capes_teses_' + str(self.ano) + '.csv'
        log_file = '/capes_teses_' + str(self.ano) + '.log'
        try:
            os.makedirs(destino_transform)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        df.to_csv(destino_transform + csv_file, sep=';', index=False, encoding='utf8',
                  line_terminator='\n', quoting=csv.QUOTE_ALL)
        self.output_length = commands.getstatusoutput('cat ' + destino_transform + csv_file + ' |wc -l')[1]
        print 'Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1)

        with open(destino_transform + log_file, 'w') as log:
            log.write('Log gerado em {}'.format(self.date.strftime("%Y-%m-%d %H:%M")))
            log.write("\n")
            log.write('Arquivo de entrada possui {} linhas de informacao'.format(int(self.input_lenght) - 1))
            log.write("\n")
            log.write('Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1))
        print('Processamento CAPES TESES {} finalizado, arquivo de log gerado em {}'.format(str(self.ano),
                                                                                            destino_transform + log_file))


def capes_teses_tranform():
    """
    Função chamada em transform.py para ajustar os dados da Capes Teses e prepará-los
    para a carga no indexador. Seta o diretório onde os arquivos a serem transformados/ajustados estão,
    passa o parâmetro - ano para a classe CapesTeses.

    PARAMETRO:
        Não recebe parâmetro.

    RETORNO:
        Função sem retorno.

    """
    PATH_ORIGEM = BASE_PATH_DATA + 'capes_teses/'
    try:
        anos = os.listdir(PATH_ORIGEM)
        anos.sort()

    except OSError:
        print('Nenhuma pasta encontrada')
        raise
    for ano in anos:

        try:
            capes_teses = CapesTeses(ano)
            capes_teses.gera_csv()
            print('Arquivo do ano, {} finalizado'.format(ano))

        except:
            print u'Arquivo do ano, {} não encontrado'.format(ano)
            raise
        print('Fim!!')
        print('\n')
