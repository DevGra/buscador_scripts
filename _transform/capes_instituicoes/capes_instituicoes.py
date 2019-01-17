# -*- coding: utf-8 -*-
import errno
import os
import sys

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, '../../../buscador_scripts/')

from utils.utils import *
import pandas as pd
import codecs
import csv
import commands
from datetime import datetime

class CapesInstituicoes(object):
    """
    A classe CapesInstituicoes é responsável pela transformaçao da base de dados(Collection - instituições),
    faz parte do processo de ETL(Extração, Transformação e Carga).

    Atributos:
        date            (datetime): data de execução deste arquivo.
        input_lenght    (int): Variável que irá guardar a quantidade de linhas do arquivo de entrada(download).
        output_length   (int): Variável que irá guardar a quantidade de linhas do arquivo de saída(transform).
        ies             (class: pandas.core.frame.DataFrame): Dataframe dos arquivos de download da capes cadastro IES, em BASE_PATH_DATA + 'capes/programas/cadastro/'.
        colunas         (dict): Dicionário das colunas do arquivo csv - programas.

    """

    def __init__(self, arquivo, nome_arquivo):
        """
        Construtor da classe CapesProgramas, recebe 2 parâmetros.

        PARAMETROS:
            arquivos        (list): Lista com todos os arquivos da pasta download -  BASE_PATH_DATA + 'capes/programas/download/'.
            nome_arquivo    (str): Nome dos arquivos da pasta download BASE_PATH_DATA + 'capes/programas/download/'.

        """
        self.date = datetime.now()
        self.arquivos = arquivo
        self.nome_arquivo = nome_arquivo
        self.input_lenght = 0
        self.output_length = 0
        self.ies = self.pega_arquivo_instituicoes_capes()
        self.colunas = [
            'AN_BASE',
            'ID_PESSOA',
            'NM_DISCENTE',
            'TP_DOCUMENTO_DISCENTE',
            'NR_DOCUMENTO_DISCENTE',
            'SituacaoDiscente',
            'IngressanteAno',
            'GrauAcademico',
            'DT_SITUACAO_DISCENTE',
            'Ano_SITUACAO_DISCENTE',
            'DT_MATRICULA_DISCENTE',
            'Ano_SITUACAO_MATRICULA',
            'DT_TITULACAO',
            'QT_MES_TITULACAO',
            'Casos_excluidos_GeoCapes',
            'Genero',
            'Idade',
            'DS_TIPO_NACIONALIDADE_DISCENTE',
            'NM_PAIS_NACIONALIDADE_DISCENTE',
            'NM_INST_FapespGei',
            'SG_ENTIDADE_ENSINO',
            'NM_ENTIDADE_ENSINO',
            'cat_insti',
            'CS_STATUS_JURIDICO',
            'DS_DEPENDENCIA_ADMINISTRATIVA',
            'CS_Natureza_Juridica',
            'DS_ORGANIZACAO_ACADEMICA_Fapesp',


        ]


    def pega_arquivo_instituicoes_capes(self):
        """
        Este método itera os arquivos de cadastro da CAPES IES em: BASE_PATH_DATA + 'capes/programas/cadastro/'
        que será agregado ao programas e elimina as colunas e linhas vazias.

        PARAMETRO:
            Não recebe parâmetro.

        RETORNO:
            Retorna o df_cad que é o dataframe de cadastro da IES Capes.

        """

        var = '/var/tmp/solr_front/collections/capes/programas/intituicoes/download'
        for root, dirs, files in os.walk(var):
            for file in files:
                arquivo = codecs.open(os.path.join(root, file), 'r')  # , encoding='latin-1')
                df_inst_temp = pd.read_csv(arquivo, sep=';', low_memory=False, encoding='latin-1')
        # eliminando as colunas vazias do csv.
        df_inst = df_inst_temp.dropna(how = 'all', axis = 'columns')
        df_inst = df_inst.dropna(how = 'all', axis = 'rows')
        print("Quantidade de linhas do arquivo {} linhas".format(df_inst.count()))

        # df_duplic = df_cad.duplicated(['NM_ENTIDADE_ENSINO_Capes'])
        # df_true = 0
        # if df_duplic == True:
        #     df_true = df_duplic
        #import pdb; pdb.set_trace()
        return df_inst

    def resolve_dicionarios(self):
        """
        Método para modificar/alterar/atualizar/remover colunas e linhas
        do dataframe e também resolver o(s) dicionário(s), pega o retorno
        do método pega_arquivo_nome, o passa como parâmetro para o
        método merge_programas, substitui os espaços em branco das colunas
        do dataframe por underline, corrige o formato das datas,
        resolve os campos para facet, busca e nuvem de palavras e faz os ajustes
        dos campos do dataframe.

        PARAMETRO:
            Não recebe parâmetro.

        RETORNO:
            Retorna um dataframe pronto para ser convertido em csv pelo método gera_csv.

        """
        parse_dates = ['DT_SITUACAO_PROGRAMA']

        for dt in parse_dates:
            df[dt] = pd.to_datetime(df[dt], infer_datetime_format=False, format='%d%b%Y:%H:%M:%S', errors='coerce')

        # df['ANO_MATRICULA_facet'] = df[df['DT_MATRICULA'].notnull()]['DT_MATRICULA'].dt.year.apply(gYear)
        #df['DT_SITUACAO_PROGRAMA'] = df[df['DT_SITUACAO_PROGRAMA'].dt.year == '2013']['DT_SITUACAO_PROGRAMA'].dt.year.apply(gYear)
        df['AN_BASE_facet'] = df['AN_BASE'].apply(gYear)
        df['NM_REGIAO_facet'] = df['NM_REGIAO'] + '|' + df['SG_UF_PROGRAMA'] + '|' + df['NM_MUNICIPIO_PROGRAMA_IES']
        df['AREA_CONHECIMENTO_facet'] = df['NM_GRANDE_AREA_CONHECIMENTO'] + '|' + df['NM_AREA_CONHECIMENTO'] + '|' + df['NM_SUBAREA_CONHECIMENTO']
        df['ANO_INICIO_PROGRAMA_facet'] = df['ANO_INICIO_PROGRAMA'].apply(gYear)

        df['DT_SITUACAO_PROGRAMA'] = df[dt].dt.strftime('%Y%m%d')
        df['DT_SITUACAO_PROGRAMA'] = df['DT_SITUACAO_PROGRAMA'].astype(str)
        df['DT_SITUACAO_PROGRAMA_facet'] = df['DT_SITUACAO_PROGRAMA'].apply(data_facet)

        df['SituacaoDiscente'] = df['NM_SITUACAO_DISCENTE']
        df['IngressanteAno'] = df['ST_INGRESSANTE']
        df['GrauAcademico'] = df['DS_GRAU_ACADEMICO_DISCENTE'].astype(str)
        df['Codigo_Mantenedora'] = df['Codigo_Mantenedora'].astype(int)

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

        df_capes = self.resolve_dicionarios()

        destino_transform = '/var/tmp/solr_front/collections/capes/instituicoes/transform'
        csv_file = '/capes_instituicoes_' + self.nome_arquivo + '.csv'
        log_file = '/capes_institucoes_' + self.nome_arquivo + '.log'

        try:
            os.makedirs(destino_transform)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        df_capes.to_csv(destino_transform + csv_file, sep=';', index=False, encoding='utf8') #'line_terminator='\n', quoting=csv.QUOTE_ALL)
        self.output_length = commands.getstatusoutput('cat' + destino_transform + csv_file + ' |wc -l')[1]
        print 'Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1)

        with open(destino_transform + log_file, 'w') as log:
            log.write('Log gerado em {}'.format(self.date.strftime("%Y-%m-%d %H:%M")))
            log.write("\n")
            log.write('Arquivo de entrada possui {} linhas de informacao'.format(int(self.input_lenght) - 1))
            log.write("\n")
            log.write('Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1))
        print('Processamento CAPES PROGRAMAS {} finalizado, arquivo de log gerado em {}'.format(self.nome_arquivo, (destino_transform + log_file)))

def capes_instituicoes_transform():
    """
    Função chamada em transform.py para ajustar os dados da Capes Programas e prepará-los
    para a carga no indexador. Seta o diretório onde os arquivos a serem transformados/ajustados estão,
    passa os parâmetros - arquivo e nome_arquivo para a classe CapesProgramas.

    PARAMETRO:
        Não recebe parâmetro.

    RETORNO:
        Função sem retorno.

    """

    PATH_ORIGEM = '/var/tmp/solr_front/collections/capes/instituicoes/download'
    try:
        arquivos = os.listdir(PATH_ORIGEM)
        arquivos.sort()
        # tamanho_arquivo = len(arquivos) - 1
        # arquivo_inicial = arquivos[0].split('.')[0]
        # arquivo_final = arquivos[tamanho_arquivo].split('.')[0]
        # nome_arquivo = arquivo_inicial +'_a_'+ arquivo_final
        for arquivo in arquivos:
            nome_arquivo = arquivo.split('.')[0]
            capes_doc = CapesInstituicoes(arquivo, nome_arquivo)
            capes_doc.gera_csv()
        print('Arquivo {} finalizado!'.format(arquivo))

    except OSError:
        print('Nenhum arquivo encontrado')
        raise
