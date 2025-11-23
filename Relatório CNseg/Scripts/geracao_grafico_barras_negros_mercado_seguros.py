"""
    Entrada: Um arquivo por semestre/ano
"""

import matplotlib.pyplot as plt
import numpy as np
from os import listdir
from pandas import read_excel, concat
from time import localtime
from re import search
import warnings

warnings.filterwarnings(action='ignore',category=UserWarning)

def le_planilha(nome):
    
    print('Lendo planilha: ',nome)
    
    df = read_excel(p) # A PLANILHA DEVE VIR COM UMA COLUNA PARA O TRIMESTRE/ANO. 
                               # NECESSÁRIO ADICIONAR.
                    
    return df


def tratando_dados(df):

    porcentagemN = [x for x in (df['PERCENTUAL NEGROS'].apply(lambda w: int(w*100))).values]
    porcentagemO = [x for x in (df['PERCENTUAL OUTROS'].apply(lambda w: int(w*100))).values]
    semestre = [x for x in df['SEMESTRE'].values]
    ano = [x for x in df['ANO'].values]
    
    listadicionario = []

    # CRIAR UMA LISTA DE DICIONARIOS
    x = np.arange(len(set(semestre))) # Cria uma sequência de valores de 0 a len(species) para o eixo x
        
    for a in set(ano): 
        for s in x:
            listadicionario.append({'Negros': porcentagemN[s], 'Outros': porcentagemO[s],'Semestre':semestre[s],'Ano':a})
    
    return listadicionario


def gera_grafico(listadicionario):

    data = f'{str(localtime().tm_mday).zfill(2)}{str(localtime().tm_mon).zfill(2)}{localtime().tm_year}{str(localtime().tm_hour).zfill(2)}{str(localtime().tm_min).zfill(2)}{str(localtime().tm_sec).zfill(2)}'
    
    # Extrair porcentagens para Homens, Mulheres e Cargos
        
    #negros = [int(item['Negros'].strip('%')) for item in listadicionario]
    negros = [int(item['Negros']) for item in listadicionario]
    outros = [int(item['Outros']) for item in listadicionario]
    semestre = [item['Semestre'] for item in listadicionario]
    ano = [item['Ano'] for item in listadicionario]
    
    width = 0.20  # the width of the bars
    
    #x = np.arange(len(semestre))  
    fig, ax = plt.subplots(layout='constrained',figsize=(10,6))

    rects1 = ax.bar(1 - width/2, negros,width,label='Negros')
    rects2 = ax.bar(1 + width/2, outros,width,label='Outros')
    
    # Configurações para os eixos
    ax.set_ylabel('Porcentagem (%)')
    ax.set_title(f'Porcentagem de Negros no corpo funcional - Mercado de Seguros - {semestre[0]}⁰ Semestre / {ano[0]}')
    ax.set_xticks([1])
    ax.set_xticklabels(semestre)
    ax.set_xlabel('Semestre')
    ax.legend(loc='upper left', ncols=3)

    # Adicionar labels nas barras    
    ax.bar_label(rects1, padding=3)
    ax.bar_label(rects2, padding=3)

    # Define o limite do eixo y comum, para ser utilizado por todas as barras (exemplo: 0 a 100%). Utilizando também o mesmo ax.
    ax.set_ylim(0, 100)
    ax.set_xlim(0,2)

    #print(semestre[0])
    plt.savefig(f'negros barras mercado de seguros_{str(semestre[0]).zfill(2)}{ano[0]}_{data}.png') # Salva no diretório do notebook

    
if __name__ == "__main__":
    
    print('Executando. Aguarde...')
    
    arquivos = listdir('.')
    planilhas = [a for a in arquivos if search(r'tabela_negros.*.xlsx',a)]
    
    for p in planilhas:
        df = le_planilha(p)
        listadict = tratando_dados(df)
        gera_grafico(listadict)        


