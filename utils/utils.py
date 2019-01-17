# coding=utf-8

import os
import datetime
import time
import re

def gYear(year):
    try:
        ano = int(year) # 2017
        digit = str(ano)[-1] # 7
        inicial = ano - int(digit) # 2010
        fim = inicial + 9 # 2019
        return "{}-{}|{}".format(inicial, fim, ano) # 2010-2019|2017
    except:
        return ''

def norm_keyword(palavras):
    if not isinstance(palavras, (unicode, str)):
        return palavras
    palavras = palavras.encode('utf8')
    palavras = re.sub(r'\d.', '', palavras)
    palavras = palavras.replace(';', ',').replace('.', ',').replace('¿', 'E').replace('[', '').replace(']', '')
    return palavras.split(',')

def data_facet(data):
    try:
        data = str(data)
        ano = data[:4]
        mes = data[4:6]
        dia = data[6:]
        
        return "{}|{}|{}".format(ano, mes, dia)
    except:
        return ''


def find_regiao(cod):
    regioes = {'Regiao Norte': [11, 12, 13, 14, 15, 16, 17],
               'Regiao Nordeste': [21, 22, 23, 24, 25, 26, 27, 28, 29],
               'Regiao Sudeste': [31, 32, 33, 35],
               'Regiao Sul': [41, 42, 43],
               'Regiao Centro-Oeste': [50, 51, 52, 53]}
    v = str(cod)[0:2]
    for reg, cod in regioes.iteritems():
        if v in str(cod):
            return reg


def find_zip():
    import requests
    from bs4 import BeautifulSoup

    url = 'http://portal.inep.gov.br/microdados'

    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    all_hrefs = soup.find_all('a')
    all_links = [link.get('href') for link in all_hrefs]
    zip_files = [dl for dl in all_links if dl and '.zip' in dl]
    for l in zip_files:
        print(l)


def list_output_files(collectiondir, transform):
    PATH_ORIGEM = '/var/tmp/inep'
    try:
        anos = os.listdir(collectiondir)
        anos.sort()
    except OSError:
        print('Nenhuma pasta encontrada')
        raise
    for ano in anos:
        # print(ano)
        var = collectiondir + '/' + ano + '/' + transform
        for root, dirs, files in os.walk(var):
            for file in files:
                if file.endswith(".csv"):
                    print os.path.join(root, file)


def find_zips_pnad():
    from ftplib import FTP

    anos = ['2011', '2012', '2013', '2014', '2015']
    ftp = FTP("ftp.ibge.gov.br")
    ftp.login()
    ftp.cwd('Trabalho_e_Rendimento')
    ftp.cwd('Pesquisa_Nacional_por_Amostra_de_Domicilios_anual')
    ftp.cwd('microdados')
    for ano in anos:
        filename = "pnad_" + ano + ".zip"
        print ano
        ftp.cwd(ano)
        file_list = ftp.nlst()

        for f in file_list:
            # apply your filters
            if "dados" in f.lower() and any(f.endswith(ext) for ext in '.zip'):
                # download file sending "RETR <name of file>" command
                # open(f, "w").write is executed after RETR suceeds and returns file binary data
                with open(filename, 'wb') as zipfile:
                    print ftp.retrbinary('RETR ' + f, zipfile.write)
        ftp.cwd('../')
    ftp.quit()


def year_gp(year):
    ano = int(year)
    digit = str(ano)[-1]
    inicial = ano - int(digit)
    fim = inicial + 9
    return "{}-{}".format(inicial, fim)


def facet_citations(citation):
    lenght = len(str(citation))
    cited = int(citation)

    digit = str(cited)[-1]
    inicial_dec = cited - int(digit)
    fim_dec = inicial_dec + 9

    digit = str(cited)[-2:]
    inicial_cent = cited - int(digit)
    fim_cent = inicial_cent + 99

    digit = str(cited)[-3:]
    inicial_mil = cited - int(digit)
    fim_mil = inicial_mil + 999

    if lenght >= 4:
        return "{}-{}|{}-{}|{}-{}|{}".format(inicial_mil, fim_mil, inicial_cent, fim_cent, inicial_dec, fim_dec,
                                             citation)
    elif lenght == 3:
        return "0-999|{}-{}|{}-{}|{}".format(inicial_cent, fim_cent, inicial_dec, fim_dec, citation)
    elif 0 < lenght <= 2:
        return "0-999|0-99|{}-{}|{}".format(inicial_dec, fim_dec, citation)
    else:
        return ''


def facet_idade(idade):
    age = int(idade)

    digit = str(age)[-1]
    inicial_dec = age - int(digit)
    fim_dec = inicial_dec + 9

    return "{}-{}|{}".format(inicial_dec, fim_dec, idade)


dicionario = {
    'cross': [12345],
    'filtros': {
        'termos': ['aaaa', 'bbbb'],
        'sexo': ['m']
    },
    'wc': {
        'termos': ['ccc', 'ddd'],
        'wordss': ['eeeee'],
        'sexo': ['MMM']
    }
}


def create_one_dict(dic):
    dict_f = {}
    for k, v in dic.iteritems():
        if type(v) == dict:
            dict_x = create_one_dict(v)
            for xkey, xv in dict_x.iteritems():
                if xkey in dict_f.keys():
                    for i in xv:
                        dict_f[xkey].append(i)
                else:
                    dict_f[xkey] = xv
        elif type(v) == list:
            if k in dict_f.keys():
                if len(dict_f[k]) > 0:
                   for i in dic[k]:
                        dict_f[k].append(i)
            else:
                dict_f[k] = v

    return dict_f # descomentar depois, esta gerando um erro

#print create_one_dict(dicionario)

# função para download de um ano especifico ou todos os anos
def download_um_ou_todos_anos(ano_ref, funcao_download, all_links):

    if ano_ref != '0':
        try:
            # funcao_download == download_enade(), passa para a função download_enade() o ano de referência
            funcao_download(ano_ref)
        except IndexError:
            print('digite ano que deseja como parametro entre (2009~2016)')
        except KeyError:
            print('Ano invalido digite ano entre (2009~2016)')
    else:
        anos = all_links.keys()
        for ano in anos:
            try:
                # funcao_download == download_enade(), passa para a função download_enade() os anos de referência(all_links)
                funcao_download(ano)
            except IndexError:
                print('digite ano que deseja como parametro entre (2009~2016)')
            except KeyError:
                print('Ano invalido digite ano entre (2009~2016)')
