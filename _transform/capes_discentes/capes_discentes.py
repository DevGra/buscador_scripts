# -*- coding: utf-8 -*-
import errno
import os
import sys
import pandas as pd
import codecs
import csv
import commands
import datetime

from settings import BASE_PATH_DATA
from utils.utils import *

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

class CapesDiscentes(object):
    """
    A classe CapesDiscentes é responsável pela transformaçao da base de dados(Collection - Discentes),
    faz parte do processo de ETL(Extração, Transformação e Carga).

    Atributos:
        date            (date): data de execução deste arquivo.
        input_lenght    (int): Variável que irá guardar a quantidade de linhas do arquivo de entrada(download).
        output_length   (int): Variável que irá guardar a quantidade de linhas do arquivo de saída(transform).
        ies             (class: pandas.core.frame.DataFrame): Dataframe dos arquivos de download da capes programas.
        cadastro        (class: pandas.core.frame.DataFrame): Dataframe dos arquivos de download da capes cadastro IES.
        colunas         (dic): Dicionário das colunas do arquivo csv.

    """

    def __init__(self, arquivos, nome_arquivo):
        """
        Construtor da classe CapesDiscentes, recebe 2 parâmetros:
        arquivos    (list): Lista com todos os arquivos da pasta download -  BASE_PATH_DATA + 'capes/discentes/download/'.
        nome_arquivo    (str): Nome dos arquivos da pasta download BASE_PATH_DATA + 'capes/discentes/download/'.

        """
        self.date = datetime.datetime.now()
        self.arquivos = arquivos
        self.nome_arquivo = nome_arquivo
        self.input_lenght = 0
        self.output_length = 0
        self.arq_prog = self.pega_arquivo_programas_download()
        self.cadastro_ies = self.pega_arquivo_cadastro_ies_capes()
        self.colunas = [
            'AN_BASE',
            'NM_GRANDE_AREA_CONHECIMENTO',
            'CD_AREA_AVALIACAO',
            'NM_AREA_AVALIACAO',
            'SG_ENTIDADE_ENSINO',
            'NM_ENTIDADE_ENSINO',
            'CS_STATUS_JURIDO',
            'DS_DEPENDENCIA_ADMINISTRATIVA',
            'NM_MODALIDADE_PROGRAMA',
            'NM_GRAU_PROGRAMA',
            'CD_PROGRAMA_IES',
            'NM_PROGRAMA_IES',
            'NM_REGIAO',
            'SG_UF_PROGRAMA',
            'NM_MUNICIPIO_PROGRAMA_IES',
            'CD_CONCEITO_PROGRAMA',
            'CD_CONCEITO_CURSO',
            'ID_PESSOA',
            'NM_DISCENTE',
            'NM_PAIS_NASCIONALIDADE_DISCENTE',
            'DS_TIPO_NACIONALIDADE_DISCENTE',
            'TP_SEXO_DISCENTE',
            'DS_FAIXA_ETARIA',
            'DS_GRAU_ACADEMICO_DISCENTE',
            'ST_INGRESSANTE',
            'NM_SITUACAO_DISCENTE',
            'DT_MATRICULA_DISCENTE',
            'DT_SITUACAO_DISCENTE',
            'QT_MES_TITULACAO',
            'NM_TESE_DISSERTACAO',
            'NM_ORIENTADOR',
            'ID_ADD_FOTO_PROGRAMA',
            'ID_ADD_FOTO_PROGRAMA_IES',
            'NR_DOCUMENTO_DISCENTE',
            'TP_DOCUMENTO_DISCENTE'
        ]

    def pega_arquivos_discentes(self):
        """
        Pega os arquivos em discentes/download em Discentes, conta as linhas de entrada do arquivo,
        adiciona cada arquivo na lista(df_auxiliar) e no final, faz a concatenação deles e os retorna

        """
        var = BASE_PATH_DATA + 'capes/discentes/download/'
        df_auxiliar = []

        print 'Lendo os arquivos CAPES Discentes......'
        for root, dirs, files in os.walk(var):
            for file in files:
                if file in self.arquivos:
                    print file
                    arquivo = codecs.open(os.path.join(root, file), 'r')  # , encoding='latin-1')
                    self.input_lenght = int(commands.getstatusoutput('cat ' + os.path.join(root, file) + ' |wc -l ')[1])
                    print('Lendo o arquivo {}.......com o total de {} linhas.'.format(file, self.input_lenght - 1))
                    df_auxiliar.append(pd.read_csv(arquivo, sep=';', low_memory=False, encoding='cp1252'))
                    #df_auxiliar = pd.read_csv(arquivo, sep=';', nrows=10000, chunksize=1000, encoding='cp1252', low_memory=False)

        df_discentes = pd.concat(df_auxiliar, sort=False)

        #----------- RENOMEAR NM_ENTIDADE_ENSINO DA FIOCRUZ PARA FUNDACAO OSWALDO CRUZ (FIOCRUZ)  ----------------
        df_discentes.replace('FUNDACAO OSWALDO CRUZ', 'FUNDACAO OSWALDO CRUZ (FIOCRUZ)', inplace=True)

        # -------- ADICIONANDO OS CAMPOS AO DF_DISCENTES PARA FAZER O MERGE COM PROGRAMAS E COM CADASTRO DE IES -------------
        df_discentes['SG_ENTIDADE_ENSINO_Capes'] = df_discentes['SG_ENTIDADE_ENSINO']
        df_discentes['NM_ENTIDADE_ENSINO_Capes'] = df_discentes['NM_ENTIDADE_ENSINO']

        return df_discentes # retornando todos os anos 2013_2015 a 2016_2017

    def pega_arquivo_programas_download(self):
        """Pega os arquivos do diretorio capes/programas, faz um append deles e os retorna"""

        var = BASE_PATH_DATA + 'capes/programas/download/'
        df_auxiliar = []
        print 'Lendo os arquivos do CAPES programas....'
        for root, dirs, files in os.walk(var):

            for file in files:
                print file
                arquivo = codecs.open(os.path.join(root, file), 'r')  # , encoding='latin-1')
                self.input_lenght += int(commands.getstatusoutput('cat ' + os.path.join(root, file) + ' |wc -l ')[1])
                print 'Arquivo de entrada possui {} linhas de informacao'.format(int(self.input_lenght) - 1)
                df_auxiliar.append(pd.read_csv(arquivo, sep=';', low_memory=False, encoding='cp1252'))
                #df_auxiliar = pd.read_csv(arquivo, sep=';', nrows=10000, chunksize=1000, encoding='cp1252', low_memory=False)

        df_programas = pd.concat(df_auxiliar, sort=False)
        # drop de algumas colunas de programas que geram duplicidade com discentes
        df_programas = df_programas.drop(['NM_GRANDE_AREA_CONHECIMENTO','CD_AREA_AVALIACAO',
        'NM_AREA_AVALIACAO', 'SG_ENTIDADE_ENSINO', 'NM_ENTIDADE_ENSINO', 'CS_STATUS_JURIDICO',
        'DS_DEPENDENCIA_ADMINISTRATIVA','NM_REGIAO', 'NM_MUNICIPIO_PROGRAMA_IES', 'NM_MODALIDADE_PROGRAMA',
        'NM_PROGRAMA_IES', 'SG_UF_PROGRAMA', 'NM_GRAU_PROGRAMA', 'CD_CONCEITO_PROGRAMA',
        'ID_ADD_FOTO_PROGRAMA_IES', 'ID_ADD_FOTO_PROGRAMA'], axis=1)


        return df_programas

    def pega_arquivo_cadastro_ies_capes(self):
        """pega os arquivos de cadastro CAPES IES que serão agregados aos Discentes"""

        var = '/var/tmp/solr_front/collections/capes/instituicoes/download'
        for root, dirs, files in os.walk(var):
            for file in files:
                arquivo = codecs.open(os.path.join(root, file), 'r')  # , encoding='latin-1')
                df_cad_temp = pd.read_csv(arquivo, sep=';', low_memory=False, encoding='latin-1')
        # eliminando as colunas vazias do csv.
        df_cad_ies = df_cad_temp.dropna(how = 'all', axis = 'columns')
        df_cad_ies = df_cad_ies.dropna(how = 'all', axis = 'rows')
        # ----- DELETANDO O AN_BASE E DS_DEPENDENCIA_ADMINISTRATIVA PARA NÃO GERAR DUAS COLUNAS IGUAIS APÓS O MERGE ------
        df_cad_ies = df_cad_ies.drop(['AN_BASE', 'DS_DEPENDENCIA_ADMINISTRATIVA', 'CS_STATUS_JURIDICO'], axis=1)

        return df_cad_ies


    def merge_dataframes(self, df):
        ''' Colunas que serão agregadas aos Discentes, segundo o modelo. Esta função
            recebe um dataframe de parametro e faz o merge dele com os arquivos
            do CAPES Programas.'''

        colunas_adicionadas = [

            u'AN_BASE',
            u'NM_GRANDE_AREA_CONHECIMENTO',
            u'NM_AREA_CONHECIMENTO',
            u'NM_AREA_BASICA',
            u'NM_SUBAREA_CONHECIMENTO',
            u'NM_ESPECIALIDADE',
            u'CD_AREA_AVALIACAO',
            u'NM_AREA_AVALIACAO',
            u'SG_ENTIDADE_ENSINO',
            u'NM_ENTIDADE_ENSINO',
            u'CS_STATUS_JURIDICO',
            u'DS_DEPENDENCIA_ADMINISTRATIVA',
            u'DS_ORGANIZACAO_ACADEMICA',
            u'NM_REGIAO',
            u'SG_UF_PROGRAMA',
            u'NM_MUNICIPIO_PROGRAMA_IES',
            u'NM_MODALIDADE_PROGRAMA',
            u'CD_PROGRAMA_IES',
            u'NM_PROGRAMA_IES',
            u'NM_PROGRAMA_IDIOMA',
            u'NM_GRAU_PROGRAMA',
            u'CD_CONCEITO_PROGRAMA',
            u'ANO_INICIO_PROGRAMA',
            u'AN_INICIO_CURSO',
            u'IN_REDE',
            u'SG_ENTIDADE_ENSINO_REDE',
            u'DS_SITUACAO_PROGRAMA',
            u'DT_SITUACAO_PROGRAMA',
            u'ID_ADD_FOTO_PROGRAMA_IES',
            u'ID_ADD_FOTO_PROGRAMA'

        ]
        print 'Fazendo o merge......'
        df_merg_dis_prog = df.merge(self.arq_prog, on=['AN_BASE', 'CD_PROGRAMA_IES'],suffixes=('_discentes', '_programas'))
        #df_merged = df.merge(self.arq_prog, how='left')
        #df_merge = df_merged.loc(colunas_adicionadas)
        #return df_merged[colunas_adicionadas]

        return df_merg_dis_prog


    def resolve_dicionarios(self):
        """
        Pega o Dataframe pega_arquivos_discentes, o passa como parâmetro para o método
        merge_dataframes - que faz o merge deles, recebe este dataframe e faz outro merge
        com o dataframe self.cadastro_IES passando com chave a coluna SG_ENTIDADE_ENSINO_Capes,
        substitui os espaços em branco dos nomes das colunas do dataframe por underline,
        corrige o formato das datas, faz os ajustes dos campos do dataframe,
        resolve os campos para facet, busca e nuvem de palavras e
        os retorna para o gera_csv.

        """
        df = self.pega_arquivos_discentes()
        df = self.merge_dataframes(df)
        # df == df_merg_dis_prog neste ponto

        df = df.merge(self.cadastro_ies, on=['SG_ENTIDADE_ENSINO_Capes','NM_ENTIDADE_ENSINO_Capes'])
        import pdb; pdb.set_trace()

        """
        COLUNAS GERADAS NA SAIDA - FAZER TRATAMENTO

        u'AN_BASE',                              u'NM_GRANDE_AREA_CONHECIMENTO',
        u'CD_AREA_AVALIACAO',                    u'NM_AREA_AVALIACAO',
        u'SG_ENTIDADE_ENSINO',                   u'NM_ENTIDADE_ENSINO',
        u'CS_STATUS_JURIDICO',                   u'DS_DEPENDENCIA_ADMINISTRATIVA',
        u'NM_MODALIDADE_PROGRAMA',               u'NM_GRAU_PROGRAMA',
        u'CD_PROGRAMA_IES',                      u'NM_PROGRAMA_IES',
        u'NM_REGIAO',                            u'SG_UF_PROGRAMA',
        u'NM_MUNICIPIO_PROGRAMA_IES',             u'CD_CONCEITO_PROGRAMA',
        u'CD_CONCEITO_CURSO',                      u'ID_PESSOA',
        u'NR_DOCUMENTO_DISCENTE',                u'TP_DOCUMENTO_DISCENTE',
        u'NM_DISCENTE',                          u'NM_PAIS_NACIONALIDADE_DISCENTE',
        u'DS_TIPO_NACIONALIDADE_DISCENTE',        u'TP_SEXO_DISCENTE',
        u'DS_FAIXA_ETARIA',                        u'DS_GRAU_ACADEMICO_DISCENTE',
        u'ST_INGRESSANTE',                          u'NM_SITUACAO_DISCENTE',
        u'DT_MATRICULA_DISCENTE',                    u'DT_SITUACAO_DISCENTE',
        u'QT_MES_TITULACAO',                        u'NM_TESE_DISSERTACAO',
        u'NM_ORIENTADOR',                           u'ID_ADD_FOTO_PROGRAMA',
        u'ID_ADD_FOTO_PROGRAMA_IES',                 u'SG_ENTIDADE_ENSINO_Capes',
        u'NM_ENTIDADE_ENSINO_Capes',                u'NM_AREA_CONHECIMENTO',
        u'NM_SUBAREA_CONHECIMENTO',                 u'NM_ESPECIALIDADE',
        u'DS_ORGANIZACAO_ACADEMICA',                u'NM_PROGRAMA_IDIOMA',
        u'ANO_INICIO_PROGRAMA',                     u'AN_INICIO_CURSO',
        u'IN_REDE',                                 u'SG_ENTIDADE_ENSINO_REDE',
        u'DS_SITUACAO_PROGRAMA',                    u'DT_SITUACAO_PROGRAMA',
        u'DS_CLIENTELA_QUADRIENAL_2017',            u'NM_AREA_BASICA',
        u'CD_INST_GEI',                             u'SG_INST_GEI',
        u'NM_INST_GEI',                             u'Codigo_do_Tipo_de_Instituicao',
        u'Tipo_de_Instituicao',                     u'Codigo_Natureza_Juridica-GEI',
        u'Nome_Natureza_Juridica-GEI',              u'CD_ORGANIZACAO_ACADEMICA-GEI',
        u'DS_ORGANIZACAO_ACADEMICA-GEI',            u'DS_ORGANIZACAO_ACADEMICA_Capes',
        u'CD_Mantenedora',                           u'NM_Mantenedora'],

        """

        # Lista com as datas que devem ser formatada
        parse_dates = ['DT_MATRICULA_DISCENTE', 'DT_SITUACAO_DISCENTE' ]

        for dt in parse_dates:
            # Percorre a lista de datas e seta o formato da data que o datetime usará para a conversão, ou seja,
            # a máscara do formato.
            df[dt] = pd.to_datetime(df[dt], infer_datetime_format=False, format='%d%b%Y:%H:%M:%S', errors='coerce')

        df['SituacaoDiscente'] = df['NM_SITUACAO_DISCENTE'].astype(str)
        df['IngressanteAno'] = df['ST_INGRESSANTE'].astype(str)
        df['GrauAcademico'] = df['DS_GRAU_ACADEMICO_DISCENTE'].astype(str)
        df['Genero'] = df['TP_SEXO_DISCENTE'].astype(str)
        df['Idade'] = df['DS_FAIXA_ETARIA'].sort_values()



        df['AN_INICIO_CURSO'] = df['AN_INICIO_CURSO'].astype(str)
        #df['ANO_INICIO_PROGRAMA'] = df[df['ANO_INICIO_PROGRAMA'].notnull()]['ANO_INICIO_PROGRAMA'].astype(str)
        #df['ANO_MATRICULA_facet'] = df[df['DT_MATRICULA'].notnull()]['DT_MATRICULA'].dt.year.apply(gYear)
        #df['DT_SITUACAO_PROGRAMA'] = df[df['DT_SITUACAO_PROGRAMA'].dt.year == '2013']['DT_SITUACAO_PROGRAMA'].dt.year.apply(gYear)

        # Criação dos campos facets
        df['AN_BASE_facet'] = df['AN_BASE'].apply(gYear)
        df['NM_REGIAO_facet'] = df['NM_REGIAO'] + '|' + df['SG_UF_PROGRAMA'] + '|' + df['NM_MUNICIPIO_PROGRAMA_IES']
        df['AREA_CONHECIMENTO_facet'] = df['NM_GRANDE_AREA_CONHECIMENTO'] + '|' + df['NM_AREA_CONHECIMENTO']
        #df['DT_SITUACAO_PROGRAMA_facet'] = df['DT_SITUACAO_PROGRAMA'].dt.year.apply(gYear)
        df['ANO_INICIO_PROGRAMA_facet'] = df['ANO_INICIO_PROGRAMA'].apply(gYear)
        #df['AN_INICIO_CURSO_facet'] = df['AN_INICIO_CURSO'].apply(gYear)

        df['DT_SITUACAO_PROGRAMA'] = df[dt].dt.strftime('%Y%m%d')
        df['DT_SITUACAO_PROGRAMA'] = df['DT_SITUACAO_PROGRAMA'].astype(str)
        df['DT_SITUACAO_PROGRAMA_facet'] = df['DT_SITUACAO_PROGRAMA'].apply(data_facet)
        df['INSTITUICAO_ENSINO_facet'] =  df['SG_ENTIDADE_ENSINO'] + '|' + df['NM_ENTIDADE_ENSINO']

        df['NM_PROGRAMA_IES_exact'] = df['NM_PROGRAMA_IES']
        df['NM_PROGRAMA_IDIOMA_exact'] = df['NM_PROGRAMA_IDIOMA']
        df[u'NM_TESE_DISSERTACAO_exact'] = df[u'NM_TESE_DISSERTACAO'].apply(norm_keyword)

        # Campos setados do cadastro CAPES IES
        df['NM_INST_FapespGei'] = df['NM_INST_GEI'].astype(str)
        df['Codigo_GEI'] = df['Codigo_GEI'].astype(str)
        df['cat_insti'] = df['Codigo_do_Tipo_de_Instituicao'].astype(int)
        df['CS_Natureza_Juridica'] = df['Nome_Natureza_Juridica-GEI'].astype(str)
        df['DS_ORGANIZACAO_ACADEMICA_Fapesp'] = df['DS_ORGANIZACAO_ACADEMICA-GEI'].astype(str)
        # df['Codigo_Natureza_Juridica_-_GEI'] = df['Codigo_Natureza_Juridica_-_GEI'].astype(int)
        # df['CD_ORGANIZACAO_ACADEMICA_-_GEI'] = df['CD_ORGANIZACAO_ACADEMICA_-_GEI'].astype(int)
        # df['Codigo_Mantenedora'] = df['Codigo_Mantenedora'].astype(int)

        #import pdb; pdb.set_trace()
        return df

    def gera_csv(self):
        """
        Pega o Dataframe de retorno do método resolve_dicionario,
        cria os arquivos de saída(.csv e .log) e o diretório de destino,
        conta as linhas do arquivo .csv e os grava no diretório de destino.

        """

        df_capes = self.resolve_dicionarios()

        destino_transform = BASE_PATH_DATA + 'capes/discentes/transform'
        csv_file = '/capes_' + self.nome_arquivo + '.csv'
        log_file = '/capes_' + self.nome_arquivo + '.log'

        try:
            os.makedirs(destino_transform)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        df_capes.to_csv(destino_transform + csv_file, sep=';', index=False, encoding='utf8',
                        line_terminator='\n', quoting=csv.QUOTE_ALL)
        self.output_length = commands.getstatusoutput('cat ' + destino_transform + csv_file + ' |wc -l')[1]
        print 'Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1)

        with open(destino_transform + log_file, 'w') as log:
            log.write('Log gerado em {}'.format(self.date.strftime("%Y-%m-%d %H:%M")))
            log.write("\n")
            log.write('Arquivo de entrada possui {} linhas de informacao'.format(self.input_lenght))
            log.write("\n")
            log.write('Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1))
        print('Processamento CAPES {} finalizado, arquivo de log gerado em {}'.format(self.nome_arquivo,
                                                                                      (destino_transform + log_file)))

def capes_discentes_transform():
    """
    Função chamada em transform.py para ajustar os dados da CAPES Discentes e prepará-los
    para a carga no indexador. Seta o diretorio onde os arquivos a serem transformados/ajustados estão,
    passa os parâmetros - arquivos e nome_arquivo para a classe CapesDiscentes.

    """
    PATH_ORIGEM = BASE_PATH_DATA + 'capes/discentes/download'
    try:
        arquivos = os.listdir(PATH_ORIGEM)
        arquivos.sort()

        arquivo_inicial = arquivos[0]
        nome_arquivo = arquivo_inicial.split('_')[0]

        capes_doc = CapesDiscentes(arquivos, nome_arquivo)
        capes_doc.gera_csv()
        print('Arquivo {} finalizado!'.format(nome_arquivo))

    except OSError:
        print('Nenhum arquivo encontrado')
        raise
