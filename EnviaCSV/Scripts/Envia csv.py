import oracledb
from datetime import date
import pandas as pd
import pysftp as sftp
import warnings
from time import time
import openpyxl
import os
import re
import lepaineldecontrole

warnings.filterwarnings('ignore','.*Failed to load.*')

def conexao_bd(senha, usuario, host, sid):
    
    oracledb.init_oracle_client()
        
    dsn = oracledb.makedsn(host=host, port=1521, sid=sid)
    
    connection = oracledb.connect(
        user=usuario,
        password=senha,
        dsn=dsn
    )

    print("Successfully connected to Oracle Database")

    return connection

def conexao_sftp():
    
    cnopts = sftp.CnOpts() # Para não utilizar chave
    cnopts.hostkeys = None # Para não utilizar chave
    s = sftp.Connection(ip, username=username, password=password, cnopts = cnopts) 
    
    return s

def executa_job_dre(ano):
    
    conexao.cursor().execute("ALTER SESSION SET nls_date_format = 'DD/MM/YY'")
    conexao.cursor().execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ',.'") 
    
    print(f'Executando PROC PKG_BI_CORPORATIVO_DRE.PRC_EXECUTA_JOBS(pano => {ano})')
    conexao.cursor().execute(f"begin PKG_BI_CORPORATIVO_DRE.PRC_EXECUTA_JOBS_DRE(pano => {ano}); end;")     
    

def executa_job_fluxo_caixa(ano):
    
    conexao.cursor().execute("ALTER SESSION SET nls_date_format = 'DD/MM/YY'")
    conexao.cursor().execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ',.'") 
    
    print(f'Executando PROC PKG_BI_CORPORATIVO_FLUXO_CAIXA.PROCEDURE PRC_EXECUTA_JOBS_FLUXO_CAIXA(pano => {ano})')
    conexao.cursor().execute(f"begin PKG_BI_CORPORATIVO_FLUXO_CAIXA.PRC_EXECUTA_JOBS_FLUXO_CAIXA(pano => {ano}); end;")  
 
    
def lista_tabela(sql):
        
    print(sql)

    cursor = conexao.cursor()

    tabela_cursor = cursor.execute(sql)

    tabela = tabela_cursor.fetchall()  # LISTA DE TUPLAS

    return cria_dataframe(tabela, cursor)


def cria_dataframe(resultado, cursor):

    # CURSOR DESCRIPTION INFORMA AS COLUNAS DO BANCO DE DADOS
    df = pd.DataFrame(resultado, columns=[desc[0] for desc in cursor.description])
    
    cursor.close()

    return df


def cria_arquivo(dataframe,nome):
    
    hoje = date.today()
    
    data_formatada = hoje.strftime("%d%m%Y")    
   
    dataframe.to_csv(nome, sep='|',index=False)

    return nome

def envia_arquivo(nome):
        
        # Listando diretório
        #files = s.listdir_attr(".")
        #for f in files:
        #    print(f)
                
        print(f'Enviando {nome}')
        s.put(nome)  # upload file to public/ on remote
        
        # FOI NECESSÁRIO FAZER MODIFICAÇÕES NO CÓDIGO do pysftp, linha 848, código sftp_client.py
        
"""         
 Função para informar os anos necessários dos arquivos de DRE e
 verificar a existência de arquivos do ano anterior       
"""
def cria_dre():    
        
        ano = date.today().year
        
        cria_dre_anoanterior(ano)
        
        # ARQUIVOS ANO ATUAL
        print('Criando DRE do ano atual')    
        lepaineldecontrole.main(diretoriopainelcontroleanoatual)
        cria_arquivo_dre(ano)       

             
def cria_dre_anoanterior(ano): # ESSA EXECUÇÃO SÓ OCORRE, DEVIDO A NECESSIDADE DE TER OS DADOS PARA O SALDO DE APLICACÕES
    
    # ROTINA ANO ANTERIOR
    print('Criando DRE do ano anterior')
    lepaineldecontrole.main(diretoriopainelcontroleanoanterior)
    
    cria_arquivo_dre(ano - 1)
    
    # PARA CONFERÊNCIA DOS DADOS DO ANO ANTERIOR - BASE REAL
    conexao.cursor().execute('TRUNCATE TABLE TBL_TMP_DRE_REAL_ANOANTERIOR')
    conexao.cursor().execute('INSERT INTO TBL_TMP_DRE_REAL_ANOANTERIOR (ORDEM,NIVEL1,NIVEL2,NIVEL3,NIVEL4,NIVEL5,TOTAL,ANOMES) SELECT ORDEM,NIVEL1,NIVEL2,NIVEL3,NIVEL4,NIVEL5,TOTAL,ANOMES FROM v_dre_real')
    conexao.commit()
            
    # PARA CONFERÊNCIA DOS DADOS DO ANO ANTERIOR - BASE ORÇADO
    conexao.cursor().execute('TRUNCATE TABLE TBL_TMP_DRE_ORCADO_ANOANTERIOR')
    conexao.cursor().execute('INSERT INTO TBL_TMP_DRE_ORCADO_ANOANTERIOR (ORDEM,NIVEL1,NIVEL2,NIVEL3,NIVEL4,NIVEL5,TOTAL,ANOMES) SELECT ORDEM,NIVEL1,NIVEL2,NIVEL3,NIVEL4,NIVEL5,TOTAL,ANOMES FROM v_dre_orcado')
    conexao.commit()           


def cria_arquivo_dre(ano):
    
        executa_job_dre(ano)
        
        global s                
        s = conexao_sftp()                                
        s.chdir('./Projetos/DRE/Dados_Auxiliares')   # temporarily chdir to bifiles
        
        dfdreorcado = lista_tabela("SELECT NIVEL1,NIVEL2,NIVEL3,NIVEL4,NIVEL5,TOTAL,ANOMES FROM V_DRE_ORCADO_QLIK")
        cria_arquivo(dfdreorcado, f'v_dre_orcado_qlik_{ano}.csv')
        envia_arquivo(f'v_dre_orcado_qlik_{ano}.csv')
        
        dfdrereal = lista_tabela("SELECT * FROM V_DRE_REAL_QLIK_COMID")
        
        cria_arquivo(dfdrereal, f'v_dre_real_qlik_{ano}.csv')
        envia_arquivo(f'v_dre_real_qlik_{ano}.csv')
        
        dfsaldoaplicacoes = lista_tabela(f"SELECT * FROM TBL_TMP_SALDO_APLICACOES")
        cria_arquivo(dfsaldoaplicacoes,f'tbl_tmp_saldo_aplicacoes_{ano}.csv')
        envia_arquivo(f'tbl_tmp_saldo_aplicacoes_{ano}.csv')
        
        s.close()


def cria_arquivo_dfc(ano):

        executa_job_fluxo_caixa(ano)
        
        ano = str(ano)
                
        # ARQUIVOS FLUXO DE CAIXA
        global s                
        s = conexao_sftp()                                
        s.chdir('./Projetos/Fluxo Caixa/Dados_Auxiliares')   # temporarily chdir to bifiles
        
        conexao.cursor().execute("create or replace view v_fluxo_caixa as select * from tbl_tmp_fluxo_caixa order by conta" )
        
        dffluxocaixa = lista_tabela("SELECT * FROM V_FLUXO_CAIXA")
        
        novocabecalho = ['DESCRICAO','CONTA']
        
        # MODIFICANDO OS NOMES DAS COLUNAS VALORMES, PARA ANO/MES
        for coluna in dffluxocaixa.columns:
            if len(re.findall(r"VALORMES[0-9]",coluna)) != 0:
                #print('Coluna: ',coluna)
                numero = re.findall(r"[0-9]{1,2}",coluna)[0]
                #print('Numero: ',numero)
                novonome = f'{str(numero).zfill(2)}/{ano}'
                novocabecalho.append(novonome)
                
                dffluxocaixa.rename(columns={f'{coluna}':f'{novonome}'},inplace=True)
        
        
        dffluxocaixa = pd.DataFrame(dffluxocaixa,columns=novocabecalho)
        #print(dffluxocaixa)                
        
        cria_arquivo(dffluxocaixa, f'v_fluxo_caixa_{ano}.csv')
        envia_arquivo(f'v_fluxo_caixa_{ano}.csv')
                    
        dfvalorcetip = lista_tabela("SELECT * FROM TBL_TMP_VALOR_CETIP")
        cria_arquivo(dfvalorcetip, f'tbl_tmp_valor_cetip_{ano}.csv') 
        envia_arquivo(f'tbl_tmp_valor_cetip_{ano}.csv')
        
        s.close()
        

def cria_dfc():
    
    cria_dfc_anoanterior()
    #cria_arquivo_dfc(date.today().year)
        
        
def cria_dfc_anoanterior():
                         
       cria_arquivo_dfc(date.today().year - 1)
       

def obtem_planilhas_controle_investimento():
    
    # DATASTAGE obtendo todos os arquivos do diretório especificado do servidor do QlikSense
    s.get_d('./Planilhas Controle de Investimentos - RESUMO',diretoriolocalplanilhasinvest)        
    
      
def cria_cdi():
    
    # ARQUIVOS CDI
    # O NOME DAS PLANILHAS DE INVESTIMENTO DEVEM CONTER O ANO com 4 dígitos e o MÊS COM 2 DÍGITOS
    global s                
    s = conexao_sftp()                                
    s.chdir('./Projetos/CDI/Dados_Auxiliares')   # temporarily chdir to bifiles
        
    obtem_planilhas_controle_investimento() # TRAZENDO AS PLANILHAS PARA A MÁQUINA LOCAL, OU SEJA, DATASTAGE  
    
    planilhas = []
    
    files = os.listdir(diretoriolocalplanilhasinvest)
        
    # LOCALIZA PLANILHAS
    for file in files:
        #print('Arquivo: ',file)
        
        if re.match(planilhasinvestimento,file):
            print('Arquivo: ',file)            
            planilhas.append(file)
    
    # CONCATENA DATAFRAMES A PARTIR DAS PLANILHAS
    if len(planilhas) > 0:
        print('Lendo planilha: ', planilhas[0]) # CRIANDO O PRIMEIRO DATAFRAME
        dfcdi = le_planilha_control_invest(diretoriolocalplanilhasinvest+planilhas[0])
        
        # CONCATENA AS PLANILHAS RESTANTES
        for i in range(len(planilhas)-1):
            print('Lendo planilha: ', planilhas[i+1])
            dfcdi1 = le_planilha_control_invest(diretoriolocalplanilhasinvest+planilhas[i+1])
            dfcdi = pd.concat([dfcdi,dfcdi1])       
        
    else:
        print('Não foram encontradas as planilhas Controle de Investimentos ano_mes_RESUMO.xlsx')
    
    ano = date.today().year
    mes = date.today().month
    
    if mes == 1:
        ano = str(ano - 1)
    else:
        ano = str(ano)
            
    cria_arquivo(dfcdi.reset_index(drop=True).sort_values(by='Anomes'),f'cdi_{ano}.csv')
    envia_arquivo(f'cdi_{ano}.csv')  
    
    return dfcdi.reset_index(drop=True).sort_values(by='Anomes')
 
def le_planilha_control_invest(planilha): # DIRETÓRIO + PLANILHA
    
    df = pd.read_excel(planilha,sheet_name='RESUMO')
    mes = re.findall(r"[0-9]{2}",df.loc[0,'Unnamed: 0'])[1]
    ano = re.search(r"[0-9]{4}", df.loc[0,'Unnamed: 0'])[0]
    
    df = df.loc[df['Unnamed: 0'] == 'TOTAL DOS FUNDOS DE INVESTIMENTOS'][['Unnamed: 4', 'Unnamed: 6']].reset_index(drop=True).loc[[0]]   
    
    df.rename(columns={
                        'Unnamed: 4':  '(%)Bench (Mês)',
                        'Unnamed: 6':  '(%)Bench (Ano)'                        
                      },inplace=True)
    
    dfreal = df.loc[0,'(%)Bench (Mês)']
    dfrealmediaacumul = df.loc[0,'(%)Bench (Ano)']
    
    real = formatar_valor(round(dfreal*100,2))
    realmediaacumul = formatar_valor(round(dfrealmediaacumul*100,2))
    
    anomes = f'{ano}{mes}'
    
    dfcdi = pd.DataFrame(
                            {
                                'Anomes' : [anomes],
                                'Realizado (%)' : [real],
                                'Orçado_Média_Anual (%)' : [100],
                                'Realizado_Média_Acumulado (%)': [realmediaacumul]
                            }                       
                        )
    
    return dfcdi   

  
def formatar_valor(valor):    
    
    #print('Valor: ',valor)
    
    if valor == '':        
        #print('Valor: ',valor)
        valor = float(str(valor).replace('','0'))       
    
    else:
        valor = float(str(valor).replace(',','.'))
    
    valor = f'{valor:_.2f}'.replace('.', ',').replace('_', '.')       
            
    return valor     

def ajusta_largura(worksheet):
    
    for col in worksheet.columns:
        #print('Col: ',col)
        max_length = 0
        for cell in col:
            #print('Cel: ',cell)
            if isinstance(cell.value, str):
                max_length = max(max_length, len(str(cell.value))) # RETORNA O MAIOR TAMANHO ENTRE 2 TAMANHOS
            elif isinstance(cell.value, int) or isinstance(cell.value, float):
                max_length = max(max_length, len(str(cell.value)))
        
        col = col[0].column_letter
        #print('1 col da tupla: ',col)
        worksheet.column_dimensions[col].width = max_length + 2

    # writer.close()
    
    return worksheet
  
# BANCO HMLG
#senhabd = 'zvga2jd0871'
#usuariobd = 'QLIK_MXM'
#host = '10.10.20.50'
#sid = 'HOMOLOG'

# BANCO PRD
senhabd = 'zvga2jd0871'
usuariobd = 'QLIK_MXM'
host = '10.10.20.61'
sid = 'MXMWEB2'

# ENVIO DOS ARQUIVOS
ip = '10.10.40.84'
username = 'qlik_admin'
password = '*wEd[<VTmb6uUql'

# PLANILHAS DE INVESTIMENTO cria_cdi()
''' 
    1) Necessário que o arquivo mantenha o mesmo padrão de nome e que fique no mesmo diretório.
    2) Os arquivos só devem ser removidos no ano seguinte
    3) As planilhas são disponilizadas em mes - 1
'''

ano = date.today().year
mes = date.today().month

if mes == 1:
    ano = str(ano - 1)
else:
    ano = str(ano)
    
planilhasinvestimento = rf"[Cc]ontrole.*[Ii]nvestimentos.*[0-9]+.*.xls.*" # O NOME DAS PLANILHAS DE INVESTIMENTO DEVEM CONTER O ANO com 4 dígitos e o MÊS COM 2 DÍGITOS
#diretoriolocalplanilhasinvest = 'E:\\DATASTAGE\\PROJETOS\\PRD\\QLIKVIEW\\EnviaCSV\\Scripts\\Planilha CDI\\' # JÁ QUE O SCRIPT RODA DENTRO DO DATASTAGE 
#diretoriolocalplanilhasinvest = 'C:\\Users\\qlik_admin\\Scripts\\EnviaCSV\\Scripts\\Planilha CDI\\'
diretoriolocalplanilhasinvest = 'G:\\Meu Drive\\Cursos e Treinamentos\\Cientista de Dados\\Treinamento Python\\EnviaCSV\\Scripts\\Planilha CDI\\'
#diretoriopainelcontroleanoanterior = 'C:\\Users\\qlik_admin\\Projetos\\DRE\\Dados_Auxiliares\\Planilhas Painel de Controle\\Ano anterior'
#diretoriopainelcontroleanoatual = 'C:\\Users\\qlik_admin\\Projetos\\DRE\\Dados_Auxiliares\\Planilhas Painel de Controle\\Ano atual'
diretoriopainelcontroleanoanterior = '.\\Ano anterior'
diretoriopainelcontroleanoatual = '.\\Ano atual'


def main():
    
    tempo_inicial = (time()) # em segundos
    
    global conexao
        
    try:   
        
        conexao = conexao_bd(senhabd, usuariobd, host, sid)
        
        #try:
        #    cria_dre()
        
        #except lepaineldecontrole.SemPainelControle as e:
        #    print(e)
        
        cria_dfc()        
        
        #cria_cdi()  
        
        conexao.close()
        
        tempo_final = (time()) # em segundos    
        print(f"Tempo Total {tempo_final - tempo_inicial} segundos")   
        
                        
    except oracledb.OperationalError as e:
        print('Erro de conexão ao Oracle')
    
if __name__ == "__main__":        
    main()
    
    
    
    