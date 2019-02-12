import os
import sys

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, '../../../buscador_scripts/')

from _download.enade_download import executa_download_enade
from _download.inep_download import executa_inep_download
from _download.pnade_download import executa_pnad
from _download.enem_download import executa_download_enem
from _download.rais import executa_rais
from _download.capes_teses_download import executa_download_capes_teses
from _download.rais_estabelecimentos import executa_rais_estabelecimentos

def executa(coll):
    '''
    Funcao para download das collections, chama outras funcoes de acordo com
    o parametro passado, se nenhum parametro for passado, exibe a mensagem
    para passar uma collection como parametro.
    ex: python download.py enade

    PARAMETROS:
    coll  (str): Nome da collection para download.

    RETORNO:
    Sem retorno.

    '''

    if coll == 'inep':
        executa_inep_download()

    elif coll == 'enade':
        executa_download_enade()

    elif coll == 'pnad':
        executa_pnad()

    elif coll == 'enem':
        executa_download_enem()

    elif coll == 'rais':
        executa_rais()

    elif coll == 'capes_teses':
        executa_download_capes_teses()

    elif coll == 'rais_estabelecimentos':
        executa_rais_estabelecimentos()

    else:
        print('Digite uma collection como parametro')


if __name__ == "__main__":
    try:
        executa(sys.argv[1])
    except IndexError:
        print('Digite o nome da collection para download ')
        #print('digite enade ou inep como parametro')
