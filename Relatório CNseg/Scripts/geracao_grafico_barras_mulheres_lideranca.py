"""
    Entrada: Um arquivo por semestre/ano
"""

import matplotlib.pyplot as plt
import numpy as np
from pandas import read_excel
from time import localtime
from os import listdir
from re import search
from pandas import concat


def le_planilha(nome):
    
    df = read_excel(nome) # A PLANILHA DEVE VIR COM UMA COLUNA PARA O TRIMESTRE/ANO. 
                               # NECESSÁRIO ADICIONAR.
    return df


def tratando_dados(df):
    
    listadicionario = []
    
    # CRIAR UMA LISTA DE DICIONARIOS
    x = np.arange(len(df['% H']))

    for i in x:
        listadicionario.append({'Homens': df['% H'][i]*100, 'Mulheres': df['% M'][i]*100, 'Cargo': df['CARGO'][i],'Semestre':df['SEMESTRE'][i],'Ano':df['ANO'][i]})
    
    return listadicionario


def gera_grafico(listadicionario):

    data = f'{str(localtime().tm_mday).zfill(2)}{str(localtime().tm_mon).zfill(2)}{localtime().tm_year}{str(localtime().tm_hour).zfill(2)}{str(localtime().tm_min).zfill(2)}{str(localtime().tm_sec).zfill(2)}'
    
    # Extrair porcentagens para Homens, Mulheres e Cargos
    homens = [item['Homens'] for item in listadicionario]
    mulheres = [item['Mulheres'] for item in listadicionario]
    cargos = [item['Cargo'] for item in listadicionario]
    semestre = [item['Semestre'] for item in listadicionario]
    ano = [item['Ano'] for item in listadicionario]
    
    width = 0.25  # the width of the bars
    x = np.arange(len(cargos))          
    fig, ax = plt.subplots(layout='constrained',figsize=(10,6))

    # Barras para Homens e Mulheres lado a lado
    rects1 = ax.bar(x - width/2, homens, width, label='Homens')
    rects2 = ax.bar(x + width/2, mulheres, width, label='Mulheres')

    # Configurações para os eixos
    ax.set_ylabel('Porcentagem (%)')
    ax.set_title(f'Porcentagem de Homens e Mulheres em cargos de liderança - {semestre[0]}⁰ Semestre / {ano[0]}')
    ax.set_xticks(x)
    ax.set_xticklabels(cargos)
    ax.legend(loc='upper left', ncols=3)

    # Adicionar labels nas barras
    ax.bar_label(rects1, padding=3)
    ax.bar_label(rects2, padding=3)

    # Define o limite do eixo y comum, para ser utilizado por todas as barras (exemplo: 0 a 100%). Utilizando também o mesmo ax.
    ax.set_ylim(0, 100)

    plt.savefig(f'mulheres cargo de liderança barras_{str(semestre[0]).zfill(2)}{ano[0]}_{data}.png') # Salva no diretório do notebook
    

if __name__ == "__main__":
    
    print('Executando. Aguarde...')
    
    arquivos = listdir('.')
    planilhas = [a for a in arquivos if search(r'mulher.*.xlsx',a)]
    
    for p in planilhas:
        print('Lendo planilha: ',p)
        df = le_planilha(p)
        listadict = tratando_dados(df)
        gera_grafico(listadict)

