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
    planilhas = [a for a in arquivos if search(r'mulheres.*.xlsx',a)]
    
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

    homens = [item['Homens'] for item in listadict]
    mulheres = [item['Mulheres'] for item in listadict]
    cargos = [item['Cargo'] for item in listadict]
    anos = [item['Ano'] for item in listadict]
    semestres = [item['Semestre'] for item in listadict]
    
    eixox = []
    
    y = np.arange(len(cargos))    
    
    for c in y:        
        for s,a in zip(semestres,anos):
            eixox.append(f'{str(s).zfill(2)}/{a}')                
            
        x1 = eixox[c]
        yh = homens[c] 
        ym = mulheres[c]
            
        print(x1,yh,ym,cargos[c])
            
        plt.plot(x1,yh,label='Homens',marker='o') # NÃO DÁ PARA EXIBIR RETA COM UM PONTO APENAS, POR ISSO O USO DO MARKER
        plt.plot(x1,ym,label='Mulheres',marker='o') 
        plt.xlabel('Semestres')
        plt.ylabel('Porcentagens(%)')
        plt.title(f'Mulheres no cargo de liderança - {cargos[c]}')
        plt.legend(loc='upper left',ncols=3)
        plt.ylim(0,100)
            
        #plt.show()
        plt.savefig(f'mulheres cargo de liderança retas - {cargos[c]}_{data}.png') # Salva no diretório do notebook 
        plt.clf()    

def tratando_dados(df):

    listadicionario = []

    # CRIAR UMA LISTA DE DICIONARIOS
    x = np.arange(len(df['% H']))

    for i in x:
        listadicionario.append({'Homens': int(df['% H'][i]*100), 'Mulheres': int(df['% M'][i]*100), 'Cargo': df['CARGO'][i],'Semestre':df['SEMESTRE'][i],'Ano':df['ANO'][i]})
        
    #print(listadicionario)
              
    return listadicionario


if __name__ == "__main__":
    
    print('Executando. Aguarde...')   
    
    df = le_planilha()
    listadict = tratando_dados(df)
    gera_grafico(listadict)   