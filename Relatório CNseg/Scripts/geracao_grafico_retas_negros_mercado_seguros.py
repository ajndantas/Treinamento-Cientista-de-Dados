"""
    Entrada: Um arquivo por semestre/ano. Todos devem ficar no diretório
"""
import matplotlib.pyplot as plt
import numpy as np
from os import listdir
from re import search
from pandas import read_excel,concat
from time import localtime
import warnings

warnings.filterwarnings(action='ignore',category=UserWarning)


def le_planilha():
    
    arquivos = listdir('.')
    planilhas = [a for a in arquivos if search(r'^tabela_negros.*.xlsx',a)]
    
    i=0
    for p in planilhas:
        print('Lendo planilha: ',p)

        if i == 0:
            df = read_excel(p) # A PLANILHA DEVE VIR COM UMA COLUNA PARA O TRIMESTRE/ANO. 
                               # NECESSÁRIO ADICIONAR.
            i += 1
            
        else:
            df = concat([df,read_excel(p)],ignore_index=True)
        
                        
    return df

def gera_grafico(listadict):

    data = f'{str(localtime().tm_mday).zfill(2)}{str(localtime().tm_mon).zfill(2)}{localtime().tm_year}{str(localtime().tm_hour).zfill(2)}{str(localtime().tm_min).zfill(2)}{str(localtime().tm_sec).zfill(2)}'

    negros = [item['Negros'] for item in listadict]
    outros = [item['Outros'] for item in listadict]
    anos = [item['Ano'] for item in listadict]
    semestres = [item['Semestre'] for item in listadict]
    
    eixox = []
    for s,a in zip(semestres,anos):
            eixox.append(f'{str(s).zfill(2)}/{a}')    
    
    print(eixox)        
    
    x1 = eixox
    yn = negros
    yo = outros
    
    plt.plot(x1,yn,label='Negros',marker='o')
    plt.plot(x1,yo,label='Outros',marker='o')
    plt.xlabel('Semestres')
    plt.ylabel('Porcentagens(%)')
    plt.title('Negros no corpo funcional do mercado segurador')
    plt.legend(loc='upper left',ncols=3)
    plt.ylim(0,100)
        
    #plt.show()
    plt.savefig(f'negros mercado de seguros retas_{data}.png') # Salva no diretório do notebook
    

def tratando_dados(df):

    listadicionario = []

    # CRIAR UMA LISTA DE DICIONARIOS
    x = np.arange(len(df['PERCENTUAL NEGROS']))

    for i in x:
        listadicionario.append({'Negros':int(df['PERCENTUAL NEGROS'][i]*100),'Outros':int(df['PERCENTUAL OUTROS'][i]*100),'Semestre':df['SEMESTRE'][i],'Ano':df['ANO'][i]})
    
    #print(listadicionario)
              
    return listadicionario


if __name__ == "__main__":
    
    print('Executando. Aguarde...')   
    
    df = le_planilha()
    listadict = tratando_dados(df)
    gera_grafico(listadict)   