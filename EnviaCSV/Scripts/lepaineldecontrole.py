import pandas as pd
from time import time
import os
import re
import sqlalchemy as sa

class SemPainelControle(Exception):
    pass

def busca_painelcontrole(diretorio):
    
    #print('Diretório: ',diretorio)
    
    files = os.scandir(path=diretorio)
    
    planilhas = []
    
    for file in files:
                
        if re.match(r".*ainel de.*ontrole.*xlsx",file.path):
            print('Arquivo: ',file.path)
            planilhas.append(file.path)
    
    # MAIS RECENTE   
    planilhas.sort(key=os.path.getmtime,reverse=True)
    
    if len(planilhas) == 0:
        raise SemPainelControle
        
    else:
        planilha = planilhas[0] # GERA EXCEÇÃO SE NÃO HOUVER PLANILHAS    
        print('Planilha Painel de controle: ', planilha)
        
        return planilha        
    


# LÊ ABA PLANO DE CONTAS
def le_planocontas(planilha):    
    
    df = pd.read_excel(planilha,sheet_name="Plano de Contas sem ponto")
    
    return df


# LÊ ABA CENTRO DE LUCRO
def le_centroscusto(planilha):
    
    df = pd.read_excel(planilha,sheet_name="Centro de lucro")
    
    return df


def inserir_no_bd(dfplanocontas,dfcentroscusto):
    
    engine = sa.create_engine(f'oracle+oracledb://{usuariobd}:{senhabd}@{host}:1521/{sid}',thick_mode=True)
    
    with engine.connect() as conn:
        query = sa.text('TRUNCATE TABLE tbl_centro_lucro')
        conn.execute(query)
        
        query = sa.text('TRUNCATE TABLE tbl_plano_conta')
        conn.execute(query)
        
    
    dfcentroscusto = pd.DataFrame(dfcentroscusto,dtype=str).replace('nan',None)
    dfcentroscusto.rename(columns={
                                    'Centro de Custo':'CENTRO_DE_CUSTO','Descrição MXM':'DESCRIÇÃO_MXM',
                                    'Classificação BP e DRE':'CLASSIFICAÇÃO_BP_E_DRE',
                                    'CÓDIGO\nGERÊNCIA':'CODIGO_GERENCIA','CÓDIGO\nSUPERINTÊNDENCIA':'CODIGO_SUPERINTENDENCIA','CÓDIGO\nDIRETORIA':'CODIGO_DIRETORIA',
                                    'Gerência':'GERÊNCIA','Superintendência':'SUPERINTENDÊNCIA','Diretoria':'DIRETORIA'
                                  },
                          inplace=True)
                    
    dfcentroscusto.to_sql('tbl_centro_lucro',engine, if_exists='append', index=False)  
    #print(dfcentroscusto)
    
    dfplanocontas = pd.DataFrame(dfplanocontas,dtype=str).replace('nan',None)
    dfplanocontas.rename(columns={
                                    'D E N O M I N A C A O':'DENOMINACAO','Natureza':'NATUREZA',
                                    'Classificação':'CLASSIFICAÇÃO','Grupo':'GRUPO',
                                    'CP x LP':'CP_X_LP','Caracteres':'CARACTERES','cod':'COD',
                                    'Grupo conta':'GRUPO_CONTA','Grupo Relatório':'GRUPO_RELATORIO','Revisão Analítica':'REVISAO_ANALITICA',
                                    'Notas':'NOTAS','Detalhe':'DETALHE'
                                  },
                          inplace=True)
    
    dfplanocontas = dfplanocontas[['CONTA','S','DENOMINACAO','NATUREZA','CLASSIFICAÇÃO','GRUPO','CP_X_LP','CARACTERES','COD','GRUPO_CONTA','GRUPO_RELATORIO','REVISAO_ANALITICA','NOTAS','DETALHE']].fillna('')
    dfplanocontas.to_sql('tbl_plano_conta',engine, if_exists='append', index=False)
    #print(dfplanocontas)
    
    engine.dispose()        


# BANCO PRD
senhabd = 'zvga2jd0871'
usuariobd = 'QLIK_MXM'
host = '10.10.20.61'
sid = 'MXMWEB2'

def cria_painelcontrole(diretoriopainelcontrole):
    
    print('Inserindo painel de controle no BD')
    planilha = busca_painelcontrole(diretoriopainelcontrole)

    dfplanocontas = le_planocontas(planilha)
    dfcentroscusto = le_centroscusto(planilha)

    #print('Plano de Contas')
    #print(dfplanocontas)

    #print()
    #print('Centros de custo')
    #print(dfcentroscusto)
    
    inserir_no_bd(dfplanocontas,dfcentroscusto)


def main(diretoriopainelcontrole):
    
    try: 
        cria_painelcontrole(diretoriopainelcontrole)
        
    except SemPainelControle:
        print(f'Não existe planilha painel de controle em {diretoriopainelcontrole}')
        
    
#if __name__ == "__main__":        
#    main()