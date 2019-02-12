# -*- coding: utf-8 -*-
import sys
from settings import BASE_PATH_DATA
import os, errno
import requests
import zipfile

all_links = {
    '2013': 'https://dadosabertos.capes.gov.br/dataset/4b0780eb-5ba4-48d4-8d6a-0b77c56f2fd7/resource/e1ebece0-1e2f-46aa-8282-6c8a1208502d/download/br-capes-btd-2013a2016-2017-12-01_2013.csv',
    '2014': 'https://dadosabertos.capes.gov.br/dataset/4b0780eb-5ba4-48d4-8d6a-0b77c56f2fd7/resource/717e84cd-3011-46e2-83bc-1dd05fbf0f1f/download/br-capes-btd-2013a2016-2017-12-01_2014.csv',
    '2015': 'https://dadosabertos.capes.gov.br/dataset/4b0780eb-5ba4-48d4-8d6a-0b77c56f2fd7/resource/332e3d8a-6764-4c16-b2b8-d10a64ccd35d/download/br-capes-btd-2013a2016-2017-12-01_2015.csv',
    '2016': 'https://dadosabertos.capes.gov.br/dataset/4b0780eb-5ba4-48d4-8d6a-0b77c56f2fd7/resource/a4a0eb59-0db1-4aa5-a13d-f0dfefd04b57/download/br-capes-btd-2013a2016-2017-12-01_2016.csv',
    '2017': 'https://dadosabertos.capes.gov.br/dataset/d920f252-9e36-4ef2-9760-c6f4611ef669/resource/bea44ea5-4233-41d8-a675-2bb9e331a003/download/ddi-br-capes-btd-2017-2018-08-01_2017.csv'

}



def download_capes_teses(ano):
    '''
    Função para download dos arquivos da CAPES Teses, esta função cria o diretório
    onde serão salvos os arquivos, faz o download dos arquivos do dicionário all_links.

    PARAMETROS:
    ano  (str): É obtido pela chave do dicionário all_links.

    RETORNO:
    Mensagem de arquivo finalizado.

    '''
    dir_destino = BASE_PATH_DATA+'capes_teses/' + str(ano) + '/download/'
    ano_str = str(ano)
    nome_arquivo = ano_str + '.csv'
    fullpath = dir_destino + nome_arquivo

    # Criando o diretorio de destino dos arquivos baixados
    try:
        os.makedirs(dir_destino)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # Fazendo o _download do arquivo
    url = all_links[ano_str]
    print '############################'
    print 'Download CAPES TESES {} em:'.format(ano)
    print fullpath
    print 'URL do arquivo:'
    print url
    print '############################'

    resp = requests.get(url, stream=True)

    with open(fullpath, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 100):  # chuck size can be larger
            if chunk:  # ignore keep-alive requests
                f.write(chunk)
        f.close()
    print "Download do ano {} finalizado".format(ano)


def executa_download_capes_teses():
    '''
    Função chamada em download.py para download dos arquivos da CAPES Teses,
    esta função chama a função download_capes_teses passando o ano como parâmetro.

    PARAMETROS:
    Sem parâmetros.

    RETORNO:
    Sem retorno.

    '''
    anos = all_links.keys()
    for ano in anos:
        try:
            download_capes_teses(ano)
        except IndexError:
            print('digite ano que deseja como parametro entre (2009~2016)')
        except KeyError:
            print('Ano invalido digite ano entre (2009~2016)')
