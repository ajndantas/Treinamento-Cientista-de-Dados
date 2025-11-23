# 
# LINHA DE EXECUÇÃO a partir de G:\Meu Drive\Cursos e Treinamentos\Cientista de Dados\Treinamento Python\.venv\Scripts
# No ambiente virtual executar. cd "G:\Meu Drive\Cursos e Treinamentos\Cientista de Dados\Treinamento Python"
# python.exe "Consumindo APIs"\consulta_titulo_receber.py F01 04 2024 030103
# 
# CONTA DO FLUXO DE CAIXA: 030103 - > SISEG, 030101 -> SNG
#

import pandas as pd
import requests
import datetime
import sys
import time
import oracledb

# PARA RETIRAR A MENSAGEM DE WARNING DE VERIFICAÇÃO DE CERTIFICADO
requests.packages.urllib3.disable_warnings()

class ParamErro(Exception):
    pass

class AnoErro(Exception):
    pass

class MesErro(Exception):
    pass

class FlxErro(Exception):
    pass

class EmpErro(Exception):
    pass

class ErroGeral(Exception):
    pass

class NotitErro(Exception):
    pass

# OBTENDO TODOS OS CENTROS DE CUSTO
def obter_centros_custo():
    
    cursor = conexao.cursor()
    
    # OBTENDO TODOS OS CENTROS DE CUSTO
    sql = "SELECT distinct cc_codigo,cc_descricao FROM ccusto_cc WHERE cc_ativo = 'S'"
    
    cccusto_cc_cursor = cursor.execute(sql)

    listacentrosdecusto = cccusto_cc_cursor.fetchall() # LISTA DE TUPLAS
    centrosdecusto = [] # LISTA DE DICIONÁRIOS

    for r in listacentrosdecusto:
        ncentrodecusto = r[0]
        descricaodocentrodecusto = r[1]
        dictcentrodecusto = {'NumerodoCentrodeCusto' : ncentrodecusto , 'DescricaodoCentrodeCusto' : descricaodocentrodecusto}
        centrosdecusto.append(dictcentrodecusto)  
    
    return centrosdecusto

def consultar_centro_custo(numerodocentrodecusto):
    
    if len(numerodocentrodecusto) != 0:       
        for cc in centrosdecusto: # LISTA DE DICIONÁRIOS DE CENTROS DE CUSTO
            if cc['NumerodoCentrodeCusto'] == numerodocentrodecusto:
                descricaodocentrodecusto = cc['DescricaodoCentrodeCusto']
                break        
        
        centrodecusto = numerodocentrodecusto + ' - ' + descricaodocentrodecusto

    else:
        centrodecusto = ''
    
    return centrodecusto

def consultar_titulo_receber(empresa, mes, ano):
    
    datapgtoinicial = '01/'+mes+'/'+ano
    #print('datapgtoinicial: ',datapgtoinicial)

    # Subtrai um dia para obter o último dia do mês anterior
    ultimo_dia = datetime.date(int(ano), int(mes)+1, 1) - datetime.timedelta(days=1)

    datapgtofinal = str(ultimo_dia.day)+'/'+mes+'/'+ano
    #print('datapgtofinal: ', datapgtofinal)
    
    response = requests.post(f'{BASE_URL}/InterfacedoContasPagarReceber/ConsultaTituloReceber', verify=False, json={
            "AutheticationToken": {
                "Username": "INTEGRACAO_MXM", 
                "Password": "Wd2&9ZL6We4!",
                "EnvironmentName": "WEBMANAGERHOM"
            },
            "Data": {
                "EmpresaEmitente": empresa,
                "DataRecebimentoInicial": datapgtoinicial,  # DATA DE PAGAMENTO INICIAL
                "DataRecebimentoFinal": datapgtofinal  # DATA DE PAGAMENTO INICIAL
            }
        }                            )
    
    return response.json()

def formatar_valor(valor):    
    
    #print('Valor: ',valor)
    
    if valor == '':        
        #print('Valor: ',valor)
        valor = float(str(valor).replace('','0'))       
    
    else:
        valor = float(str(valor).replace(',','.'))
    
     valor = f'{valor:_.2f}'.replace('.', ',').replace('_', '.')       
            
    return valor    
    
def conexao_bd(senha, usuario, host, sid):
        
    connection = oracledb.connect(
        user = usuario,
        password = senha,
        dsn = host+'/'+sid
    )
    
    #print("Successfully connected to Oracle Database")
    
    return connection

def criar_dataframe(tituloareceber):

    dftituloareceber = pd.DataFrame(tituloareceber)        
    dftituloareceber = dftituloareceber.sort_values(by='codigoclientefornecedor').reset_index(drop=True)    
    
    return dftituloareceber
    
def criar_excel(dftituloareceber):

    dftituloareceber.to_excel('consulta titulo a receber.xlsx')

def filtrar_fluxocaixa(interfacecontaspagarreceber): # INDICES DOS DICIONÁRIOS COM O FLUXO DE CAIXA

    indice = []

    # PARA REQUEST COMPLETO        
    for t in interfacecontaspagarreceber:

        # LISTA DE DICIONÁRIOS COM AS CONTAS DE FLUXO DE CAIXA
        interfacegrupopagarreceber = t['InterfaceGrupoPagarReceber']
        
        for c in interfacegrupopagarreceber:
            pcontadofluxodecaixa = c['ContadoFluxodeCaixa']

            if pcontadofluxodecaixa == contadofluxodecaixa:
                indice.append(interfacecontaspagarreceber.index(t))
                break

    print('Total de Titulos: ', len(indice))
    
    if (len(indice)) == 0:
        raise NotitErro
    
    return indice

    
def jsonparsing(json):

    tempo_inicial = (time.time())  # em segundos
    
    centrodecusto = ''
    
    tituloareceber = []
    
    interfacecontaspagarreceber = json['Data']['InterfacedoContasPagarReceber']
    
    indice = filtrar_fluxocaixa(interfacecontaspagarreceber)
    
    k = 0
    
    for l in indice:
        k = k+1
        
        i = interfacecontaspagarreceber[l]
                
        # SEÇÃO TÍTULO A RECEBER
        codigoclientefornecedor = i['CodigoClienteFornecedor']
        descricaodoclientefornecedor = i['DescricaodoClienteFornecedor']
        numerodotitulo = i['NumerodoTitulo']
        documentofiscal = i['DocumentoFiscal']
        descricaodostatusdotitulo = i['DescricaodoStatusdoTitulo']
        descricaodaempresaemitente = i['DescricaodaEmpresaEmitente']
        descricaodafilial = i['DescricaodaFilial']
        descricaodaempresarecebedora = i['DescricaodaEmpresaRecebedora']
        tipodetitulo = i['TipodeTitulo']
        descricaodotipodetitulo = i['DescricaodoTipodeTitulo']
        tipodecobranca = i['TipodeCobranca']
        descricaodotipodecobranca = i['DescricaodoTipodeCobranca']
        pedido = i['Pedido']
        datadeemissao = datetime.datetime.strptime(
            i['DatadeEmissao'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        datadevencimento = datetime.datetime.strptime(
            i['DatadeVencimento'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        datadecompetencia = datetime.datetime.strptime(
            i['DatadeCompetencia'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        observacao = i['Observacao']
        valordotitulo = formatar_valor(i['ValordoTitulo'])
        valorcorrigido = formatar_valor(i['ValorCorrigido'])
        #print('Valor Multa: ', i['ValordeMulta'])
        valordemulta = formatar_valor(i['ValordeMulta'])
        
        # SEÇÃO PAGAMENTO
        contadepagamento = i['ContadePagamento']
        agencia = i['Agencia']
        nomebancopagamento = i['NomeBancoPagamento']
        documentodepagamento = i['DocumentodePagamento']
        datadepagamento = datetime.datetime.strptime(
            i['DatadePagamento'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        valorpago = formatar_valor(i['ValorPago'])        
        
        vnumerodotitulo = numerodotitulo
        
        # TRATAR OS CASOS DE FATURAS NÃO NUMERICAS, EX: 247904A PARA SER PELA TABELA DE TITULOS A RECEBER
        # Titulo = 247904A
        
        # TABELA DE ITENS DE FATURAMENTO. AQUI NÃO EXISTEM TÍTULOS COM LETRA EM SEU CÓDIGO
        sql = "select ifat_cdfatura as fatura, ifat_descricao as descricao, ifat_quantidade as quantidade, ifat_precoinf as precounitario, ifat_tpoper as tipooperacao, ifat_noccusto as centrodecusto, ifat_vlrprod as valortotal from itfatura_ifat ifat where ifat_cdfatura = :numerodotitulo"
        
        if vnumerodotitulo.isdigit():
            print(str(k) + ' Titulo: ',vnumerodotitulo)
            
            cursor = conexao.cursor()
            cursor = cursor.execute(sql,numerodotitulo = vnumerodotitulo)
            tupla = cursor.fetchall()
                        
        elif vnumerodotitulo[0:len(vnumerodotitulo) - 1].isdigit():
            #print('Titulo antigo: ',vnumerodotitulo)
            
            vnumerodotitulo = vnumerodotitulo[0:len(numerodotitulo) - 1]
            print(str(k) + ' Titulo: ',vnumerodotitulo)
            
            cursor = conexao.cursor()
            cursor = cursor.execute(sql,numerodotitulo = vnumerodotitulo)
            tupla = cursor.fetchall()
            
        else:
            continue       
        
        # SEÇÃO APROPRIAÇÃO (PRODUTOS). MAIS DE UMA OCORRÊNCIA POR TÍTULO. PEGANDO DO FATURAMENTO
        for g in tupla:                               
                
            valordogrupo = formatar_valor(g[6])
            centrodecusto = consultar_centro_custo(g[5])
            item = g[1]
            quantidade = g[2]
            valorunitario = formatar_valor(g[3])           
            
            for j in i['InterfaceGrupoPagarReceber']:
                                
                if j['NumerodoCentrodeCusto'] == centrodecusto:
                    break
            
            codigodogrupo = j['CodigodoGrupo']
            descricaodogrupo = j['DescricaodoGrupo']
            contadofluxodecaixa = j['ContadoFluxodeCaixa']  
                                                        
            # DE CENTRO DE CUSTO PARA BAIXO, PEGAR DO FATURAMENTO                                    
            titulo = {
                    'titulo': vnumerodotitulo,
                    'documentofiscal': documentofiscal,
                    'codigoclientefornecedor': codigoclientefornecedor,
                    'fornecedor': descricaodoclientefornecedor,
                    'statusdotitulo': descricaodostatusdotitulo,
                    'empresaemitente': descricaodaempresaemitente,
                    'filial': descricaodafilial,
                    'empresarecebedora': descricaodaempresarecebedora,
                    'pedidofaturamento' : pedido,
                    'tipodetitulo': tipodetitulo +' - '+ descricaodotipodetitulo,
                    'tipodecobranca': tipodecobranca +' - '+ descricaodotipodecobranca,
                    'emissao': datadeemissao,
                    'vencimento': datadevencimento,
                    'competencia': datadecompetencia,
                    'observacao': observacao,
                    'valordotitulo': valordotitulo,
                    'valorcorrigido': valorcorrigido,
                    'valordemulta': valordemulta,
                    'contadepagamento': contadepagamento,
                    'agencia': agencia,
                    'nomebancopagamento': nomebancopagamento,
                    'documentodepagamento': documentodepagamento,
                    'pagamento': datadepagamento,
                    'valorpago': valorpago,
                    'codigodogrupo': codigodogrupo,
                    'grupoderecebimento': descricaodogrupo,
                    'contadofluxodecaixa': contadofluxodecaixa,                    
                    'centrodecusto': centrodecusto,
                    'valortotal': valordogrupo,
                    'descricaoitem' : item,
                    'quantidade' : quantidade,
                    'valorunitario' : valorunitario                 
                }            

            tituloareceber.append(titulo)           
    
    tempo_final = (time.time())  # em segundos
    print('Tempo jsonparsing: ', tempo_final - tempo_inicial)
            
    return tituloareceber    

# MAIN
#
# NumeroTitulo: 252224, Pedido de Faturamento: 262684 

global conexao
global centrosdecusto

# BANCO
senhabd = 'MXM_HMLG#$01'
usuariobd = 'MXM_HMLG'
host = 'srv2033'
sid = 'HOMOLOG'

# AMBIENTE 
BASE_URL = 'https://mxm-hmlg.cnseg.org.br/WebManagerHOM/api'

tempo_inicial = (time.time()) # em segundos

try:    
    
    # TESTA QTD DE PARÂMETROS
    if len(sys.argv) != 5:
        raise ParamErro

    else:
        global contadofluxodecaixa
            
        empresa = sys.argv[1]
        mes = sys.argv[2]
        ano = sys.argv[3]
        contadofluxodecaixa = sys.argv[4]
        
        if len(empresa) != 3:
            raise EmpErro
                            
        # Testa contadofluxodecaixa
        elif len(contadofluxodecaixa) < 6 or contadofluxodecaixa.isdigit() == False:
            raise FlxErro
            
        # Testa mês    
        elif len(mes) != 2 or mes is None or mes.isdigit() == False:
            raise MesErro

        # Testa ano
        elif len(ano) != 4 or ano is None or ano.isdigit() == False:
            raise AnoErro
                
        else:  
            json = consultar_titulo_receber(empresa, mes, ano)
            #print('JSON: ',json)
            
            if len(json['Messages']) != 0:
                raise ErroGeral
            
            else:
                conexao = conexao_bd(senhabd, usuariobd, host, sid)
                
                centrosdecusto = obter_centros_custo()
                
                tituloareceber = jsonparsing(json)             
                
                dftituloareceber = criar_dataframe(tituloareceber)                
                criar_excel(dftituloareceber)
                
                conexao.close()
    
    tempo_final = (time.time()) # em segundos
    
    print(f"Tempo Total {tempo_final - tempo_inicial} segundos")

except EmpErro:
    print("O código da empresa deve possuir 3 caracteres")
    
except ParamErro:
    print("Número incorreto de parâmetros. Devem ser 4 Empresa Mêspagamento Anopagamento ContaFLuxodeCaixa (Ex:python.exe consulta_titulo_receber.py F01 01 2024 030101) ")

except FlxErro:
    print("A conta do fluxo de caixa deve ter pelo menos 6 dígitos")   

except MesErro:
    print("O mês deve ter dois dígitos")

except AnoErro:
    print("O ano deve ter quatro dígitos")

except NotitErro:
    print("Sem título para os argumentos passados")

except ErroGeral:
    print(json['Messages'][0]['Message'])