import os
import sys
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, '../../../buscador_scripts/')
from _download.enade_download import executa_download_enade
from _download.inep_download import executa_inep_download
from _download.pnade_download import executa_pnad

def executa(coll):


    if coll == 'inep':
        executa_inep_download()


    elif coll == 'enade':
        executa_download_enade()

    elif coll == 'pnad':
        executa_pnad()

    else:
        print('digite enade ou inep como parametro')


if __name__ == "__main__":
    try:
        executa(sys.argv[1])
    except IndexError:
        print('digite enade ou inep como parametro')