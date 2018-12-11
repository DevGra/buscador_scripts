# -*- coding: utf-8 -*-
import sys

sys.path.insert(0, '../../../buscador_scripts/')

from settings import BASE_PATH_DATA
import os, errno
import requests
import zipfile
#from utils.utils import download_all_links

"""
Este script faz o download dos arquivos do ENADE e descompacta-os na pasta dos respectivos anos.
Deve-se escolher o ano de inicio e o ano final dos arquivos a serem baixados.
Antes de 2009, a nomenclatura e formato dos arquivos nao batem.

"""

all_links = {
    # '2004': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2004.zip',
    # '2005': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2005.zip',
    # '2006': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2006.zip',
    # '2007': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2007.zip',
    # '2008': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2008.zip',
    # '2009': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2009.zip',
    # '2010': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2010.zip',#
    # '2011': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2011.zip',#
    # '2012': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2012.zip',#
    # '2013': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2013.zip',
    # '2014': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2014.zip',
    # '2015': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2015.zip',
    '2016': 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2016_versao_28052018.zip'
}

def download_enade(ano):
    dir_destino = BASE_PATH_DATA + 'enade/' + str(ano) + '/download/'
    mbyte = 1024 * 1024
    ano_str = str(ano)
    nome_arquivo = ano_str + '.zip'
    fullpath = dir_destino + nome_arquivo

    # Criando o diretorio de destino dos arquivos baixados
    try:
        os.makedirs(dir_destino)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # Fazendo o _download do arquivo
    url = all_links[ano_str]
    print(url)
    resp = requests.get(url, stream=True)

    fsize = int(resp.headers['content-length'])
    print 'Downloading %s (%sMb)' % (fullpath, fsize / mbyte)
    # Gravando o arquivo zip
    with open(fullpath, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 100):  # chuck size can be larger
            if chunk:  # ignore keep-alive requests
                f.write(chunk)
        f.close()

    archive = zipfile.ZipFile(fullpath, 'r')
    archive.extractall(dir_destino)
    archive.close()

    os.remove(fullpath)
    print "Download do ano {} finalizado".format(ano)

def executa_download_enade():
    #download_all_links(ano_ref, download_enade, all_links) # funcão em utils para download dos anos
    anos = all_links.keys()
    for ano in anos:
        try:
            download_enade(ano)
        except IndexError:
            print('digite ano que deseja como parametro entre (2009~2016)')
        except KeyError:
            print('Ano invalido digite ano entre (2009~2016)')

# if __name__ == "__main__":
#     anos = all_links.keys()
#     for ano in anos:
#         try:
#             download_enade(ano)
#         except IndexError:
#             print('digite ano que deseja como parametro entre (2009~2016)')
#         except KeyError:
#             print('Ano invalido digite ano entre (2009~2016)')
#
