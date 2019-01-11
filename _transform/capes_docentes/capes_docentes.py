# -*- coding: utf-8 -*-
import errno
import os
import sys
import pandas as pd
import codecs
import csv
import commands
from datetime import datetime

from settings import BASE_PATH_DATA
from utils import *

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, '../../../buscador_scripts/')

class CapesDocentes(object):
    """
    A classe CapesDocentes é responsável pela transformaçao da base de dados(Collection - docentes),
    faz parte do processo de ETL(Extração, Transformação e Carga).

    Atributos:
        date            (date): data de execução deste arquivo.
        input_lenght    (int): Variável que irá guardar a quantidade de linhas do arquivo de entrada(download).
        output_length   (int): Variável que irá guardar a quantidade de linhas do arquivo de saída(transform).
        colunas         (dic): Dicionário das colunas do arquivo csv.

    """

    def __init__(self, arquivos, nome_arquivo):
        """
        Construtor da classe CapesDocentes, necessita de 2 parâmetros:
        arquivos        (list): Lista com todos os arquivos da pasta download -  BASE_PATH_DATA + 'capes/docentes/download/'.
        nome_arquivo    (str): Nome dos arquivos da pasta download BASE_PATH_DATA + 'capes/docentes/download/'.

        """

        self.date = datetime.now()
        self.arquivos = arquivos
        self.nome_arquivo = nome_arquivo
        self.input_lenght = 0
        self.output_length = 0

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
            'TP_DOCUMENTO_DISCENTE',

        ]

    def pega_arquivo_docente(self):
        ''' Pega os arquivos em docentes/download, conta as linhas de entrada do arquivo,
            adiciona cada arquivo na lista(df_auxiliar), faz a concatenação deles e os retorna

        '''

        var = BASE_PATH_DATA + 'capes/docentes/download/'
        df_auxiliar = []
        for root, dirs, files in os.walk(var):
            for file in files:
                if file in self.arquivos:
                    arquivo = codecs.open(os.path.join(root, file), 'r')  # , encoding='latin-1')
                    self.input_lenght += int(
                        commands.getstatusoutput('cat ' + os.path.join(root, file) + ' |wc -l ')[1])
                    print 'Arquivo de entrada possui {} linhas de informacao'.format(int(self.input_lenght) - 1)
                    df_auxiliar.append(pd.read_csv(arquivo, sep=';', low_memory=False, encoding='cp1252'))
                    #df_auxiliar = pd.read_csv(arquivo, sep=';', nrows=3000, chunksize=3000, encoding='latin-1', low_memory=False)
        #import pdb;pdb.set_trace()  #para testar o código

        df_concat = pd.concat(df_auxiliar)
        return df_concat

    def resolve_dicionarios(self):
        """
        Pega o Dataframe de retorno do método pega_arquivo_docente, resolve os campos para facet
        e os retorna para o gera_csv.

        """

        df = self.pega_arquivo_docente()
        df['NM_REGIAO_facet'] = df['NM_REGIAO'] + '|' + df['SG_UF_PROGRAMA'] + '|' + df['NM_MUNICIPIO_PROGRAMA_IES']
        df['NM_AREA_CONHECIMENTO_facet'] = df['NM_GRANDE_AREA_CONHECIMENTO'] + '|' + df['NM_AREA_CONHECIMENTO']
        #import pdb; pdb.set_trace()

        return df

    def gera_csv(self):
        """
        Pega o Dataframe de retorno do método resolve_dicionario, cria o diretório de destino,
        conta as linhas do arquivo de saída e grava o .csv e o .log no diretório de destino.

        """

        df_capes = self.resolve_dicionarios()

        destino_transform = BASE_PATH_DATA + 'capes/docentes/transform'
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

def capes_docentes_transform():
    """
    Função chamada em transform.py para ajustar os dados da  CAPES Docentes e prepará-los
    para a carga no indexador. Seta o diretorio onde os arquivos a serem transformados/ajustados estão,
    e passa os parâmetros - arquivos e nome_arquivo para a classe CapesDocentes.

    """

    PATH_ORIGEM = BASE_PATH_DATA + 'capes/docentes/download'

    try:
        arquivos = os.listdir(path_origem)
        arquivos.sort()
        arquivo_inicial = arquivos[0]
        nome_arquivo = arquivo_inicial.split('_')[0]
        capes_doc = CapesDocentes(arquivos, nome_arquivo)
        capes_doc.gera_csv()
        print('Arquivo {} finalizado!'.format(nome_arquivo))

    except OSError:
        print('Nenhum arquivo encontrado')
        raise
