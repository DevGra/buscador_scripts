# -*- coding: utf-8 -*-
import errno
import os
import sys
# from utils import *
import pandas as pd
import codecs
import csv
import commands
from datetime import datetime

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, '../../../buscador_scripts/')


class CapesDocentes(object):

    def __init__(self, arquivos, nome_arquivo):

        self.date = datetime.now()
        self.arquivos = arquivos
        self.nome_arquivo = nome_arquivo
        self.input_lenght = 0
        self.output_length = 0
        # conta as linhas corretas dos arquivos na pasta download. é usado para subtrair as linhas de saída.
        self.qtde_linhas_corretas = 0
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

    def pega_arquivo_nome(self):

        var = '/var/tmp/solr_front/collections/capes/docentes/download/'
        df_auxiliar = []
        for root, dirs, files in os.walk(var):
            for file in files:
                if file in self.arquivos:

                    arquivo = codecs.open(os.path.join(root, file), 'r')  # , encoding='latin-1')
                    self.input_lenght = int(commands.getstatusoutput('cat ' + os.path.join(root, file) + ' |wc -l ')[1])
                    print('Lendo o arquivo {}.......com o total de {} linhas.'.format(file, self.input_lenght - 1))
                    df_auxiliar.append(pd.read_csv(arquivo, sep=';', low_memory=False, encoding='cp1252'))
                    self.qtde_linhas_corretas += (self.input_lenght - 1)
        print 'Arquivo de entrada possui {} linhas de informacao'.format(self.qtde_linhas_corretas)
        # import pdb;pdb.set_trace()  #para testar o código
        df_concat = pd.concat(df_auxiliar)
        return df_concat

    def gera_csv(self):

        df_capes = self.pega_arquivo_nome()

        destino_transform = '/var/tmp/solr_front/collections/capes/docentes/transform'
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
            log.write('Arquivo de entrada possui {} linhas de informacao'.format(self.qtde_linhas_corretas))
            log.write("\n")
            log.write('Arquivo de saida possui {} linhas de informacao'.format(int(self.output_length) - 1))
        print('Processamento CAPES {} finalizado, arquivo de log gerado em {}'.format(self.nome_arquivo,
                                                                                      (destino_transform + log_file)))


def capes_docentes_transform():
    path_origem = '/var/tmp/solr_front/collections/capes/docentes/download'
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
