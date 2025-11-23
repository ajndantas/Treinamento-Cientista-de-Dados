# 
# LINHA DE EXECUÇÃO a partir de G:\Meu Drive\Cursos e Treinamentos\Cientista de Dados\Treinamento Python\Consulta receita despesa\consulta_receita_despesa\Scripts
# 
# TEMPO DE EXECUÇÃO: 5 MINUTOS
#
# EXISTE NO MÉTODO MAIN, UMA MANEIRA DE SE FORÇAR CARGAS.
#
# OS MESES VÃO SENDO INSERIDOS NO BI, A MEDIDA QUE VÃO SENDO ENCERRADOS
#
# 3 TABELAS SERÃO ESCRITAS: TBL_COB_FENASEG e TBL_TMP_RECEITA_SNG e TBL_TMP_FLUXO_CAIXA_SNG
#
# NOVA FORMA DE EXECUTAR: Dentro da pasta C:\Users\qlik_admin\Scripts\consulta_receita_despesa\Scripts como administrador 
# python.exe consulta_receita_sng_siseg.py
# 
# SOMENTE PARA ESSAS CONTAS DO FLUXO DE CAIXA: 030103 - > SISEG, 030101 -> SNG
#
# Consultas por mês e ano de pagamento. Cuidado, diferente de competencia, que é o ulitmo dia, do mês anterior ao pagamento
#
#
from pandas import DataFrame
from requests import post
from datetime import timedelta, date, datetime
from time import time
import oracledb
import openpyxl
from urllib3 import disable_warnings
from openpyxl import load_workbook
import os
import re
import warnings

warnings.filterwarnings('ignore','.*Failed to load.*')

# PARA RETIRAR A MENSAGEM DE WARNING DE VERIFICAÇÃO DE CERTIFICADO
disable_warnings()

tempo_inicial = (time())  # em segundos

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

class ErroAPI(Exception):
    pass

class ErroGeralAPI(Exception):
    pass

class NotitErro(Exception):
    pass

class Mesinvalido(Exception):
    pass

class Anoinvalido(Exception):
    pass

#class Competencia(Exception):
#    pass

def formatar_valor(valor):    
    
    if valor == '':
        
        #print('Número do Título com desconto e valor '': ',numerodotitulo)
                
        valor = float(str(valor).replace('','0'))
        valor = f'{valor:,.2f}'.replace('.', ',')
        
    elif valor == '0':
        valor = float(valor)
        valor = str(f'{valor:,.2f}').replace('.', ',')
            
    else:
        valor = float(str(valor).replace(',','.'))
        valor = f'{valor:_.2f}'.replace('.', ',').replace('_', '.') 
        
        #print('Número do Título com desconto: ',numerodotitulo)  
        #print('Valor diferente de nada: ',valor)       
    
    #print('Valor: ',valor) 
               
    return valor  


def formatar_quantidade(quantidade):    
    
    if quantidade == '':        
        quantidade = int(str(quantidade).replace('','0'))       
    
    else:
        
        quantidade = int(quantidade)
                
        quantidade = f'{quantidade:_}'.replace('_','.')
        #print ('Quantidade: ',quantidade)        
            
    return quantidade


# OBTENDO TODOS OS CENTROS DE CUSTO
def obter_centros_custo(indice,interfacecontaspagarreceber):
    
    cursor = conexao.cursor()
    codigoccusto = []
        
    for l in indice:
        
        for i in interfacecontaspagarreceber[l]['InterfaceGrupoPagarReceber']:
        
            codigo = i['NumerodoCentrodeCusto']
            codigoccusto.append(codigo)
    
    codigoccusto = tuple(set(codigoccusto))
    
    print('Tamanho: ',len(codigoccusto))    

    if len(codigoccusto) == 1:
        codigoccusto = f"('{codigoccusto[0]}')"
    
    print('Código ccusto: ', codigoccusto)
    
    # OBTENDO TODOS OS CENTROS DE CUSTO
    sql = f"SELECT distinct cc_codigo,cc_descricao FROM MXM_PRD.ccusto_cc WHERE cc_ativo = 'S' and cc_codigo IN {codigoccusto}"
    
    cccusto_cc_cursor = cursor.execute(sql)

    listacentrosdecusto = cccusto_cc_cursor.fetchall() # LISTA DE TUPLAS 

    #print('Lista centros de custo')
    #print(listacentrosdecusto)
    
    for r in listacentrosdecusto:
        ncentrodecusto = r[0]
        descricaodocentrodecusto = r[1]
        centrosdecusto[ncentrodecusto] = descricaodocentrodecusto
        
    
    return centrosdecusto

# PAGA A FORNECEDOR E RECEBE DE CLIENTE
def obter_uf_cliente(indice,interfacecontaspagarreceber):
    
    cursor = conexao.cursor()
    clicodigo = []
    clicodigodict = {}
    
    for l in indice:
        
        i = interfacecontaspagarreceber[l]
        
        codigocli = i['CodigoClienteFornecedor']
        clicodigo.append(codigocli)
    
    clicodigo = tuple(clicodigo)    
    
    #print('Clicodigo')
    #print(clicodigo)

    sql = 'TRUNCATE TABLE TBL_TMP_CODCLI_UF'
    cursor.execute(sql)

    for c in clicodigo:
        sql = f"INSERT INTO TBL_TMP_CODCLI_UF (CLI_CODIGO,UF) SELECT CLI_CODIGO,CLI_UF as UF FROM MXM_PRD.cliente_cli WHERE cli_codigo = '{c}'"
        cursor.execute(sql)
    
    sql = 'commit'
    cursor.execute(sql)

    #sql = f'SELECT cli_codigo,cli_uf as uf FROM MXM_PRD.cliente_cli WHERE cli_codigo IN {clicodigo}'

    sql = f'SELECT cli_codigo,uf FROM tbl_tmp_codcli_uf'
    
    uf_cursor = cursor.execute(sql)
    uftuplelist = uf_cursor.fetchall()

    for p in uftuplelist:
        clicodigodict[p[0]] = p[1]            
    
    return clicodigodict
   

def consultar_centro_custo(numerodocentrodecusto):        
    
    if len(numerodocentrodecusto) != 0:       
        descricaodocentrodecusto = centrosdecusto[numerodocentrodecusto]                
        centrodecusto = numerodocentrodecusto + ' - ' + descricaodocentrodecusto

    else:
        centrodecusto = ''
    
    return centrodecusto

def consultar_titulo_receber(empresa, mes, ano):
    
    #mes = '01' # FORÇADO PARA O SNG ANO INTEIRO
    
    datapgtoinicial = '01/'+mes+'/'+ano
    #print('datapgtoinicial: ',datapgtoinicial)

    # Subtrai um dia para obter o último dia do mês anterior
    if int(mes) != 12:
        ultimo_dia = date(int(ano), int(mes)+1, 1) - timedelta(days=1)
        
    else:
        ultimo_dia = date(int(ano)+1, 1, 1) - timedelta(days=1)

    
    datapgtofinal = str(ultimo_dia.day)+'/'+mes+'/'+ano
    #datapgtofinal = '31/12/'+ano # FORÇADO PARA O SNG ANO INTEIRO
    
    #print('datapgtofinal: ', datapgtofinal)
    
    response = post(f'{BASE_URL}/InterfacedoContasPagarReceber/ConsultaTituloReceber', verify=False, json={ # A MÁQUINA DO DATASTAGE PRECISA TER ACESSO A ESSA REDE.
                "AutheticationToken": {
                "Username": loginapi, 
                "Password": senhaapi,
                "EnvironmentName": ambiente
            },
            "Data": {
                "EmpresaEmitente": empresa,
                "DataRecebimentoInicial": datapgtoinicial,  # DATA DE PAGAMENTO/VENCIMENTO INICIAL. MÊS SEGUINTE AO DA COMPETÊNCIA
                "DataRecebimentoFinal": datapgtofinal  # DATA DE PAGAMENTO/VENCIMENTO FINAL. MÊS SEGUINTE AO DA COMPETÊNCIA
            }
        }                            )
    
    return response.json()

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

def criar_dataframe(tituloareceber):

    dftituloareceber = DataFrame(tituloareceber)
    dftituloareceber = dftituloareceber.sort_values(by=['codigocliente','titulo']).reset_index(drop=True)
    
    #print(dftituloareceber)    
    
    return dftituloareceber

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
    
    else:
        return indice
    
    
def jsonparsing(json):

    tempo_inicial = (time())  # em segundos
    
    centrodecusto = ''
    
    tituloareceber = []
            
    interfacecontaspagarreceber = json['Data']['InterfacedoContasPagarReceber']
    
    indice = filtrar_fluxocaixa(interfacecontaspagarreceber)
    
    clicodigodict = obter_uf_cliente(indice,interfacecontaspagarreceber)
    centrosdecusto = obter_centros_custo(indice,interfacecontaspagarreceber)
    
    #print(centrosdecusto)
       
    #print('Clicodigodict: ',clicodigodict)
    
    k = 0
    l = 0
    
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
        datadeemissao = datetime.strptime(
            i['DatadeEmissao'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        datadevencimento = datetime.strptime(
            i['DatadeVencimento'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        datadecompetencia = datetime.strptime(
            i['DatadeCompetencia'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        observacao = i['Observacao']
        valordotitulo = formatar_valor(i['ValordoTitulo'])
        valorcorrigido = formatar_valor(i['ValorCorrigido'])
        
        valordesconto = formatar_valor(i['ValordeDesconto']) 
            
        if len(str(i['DatadoDesconto'])) != 0:
            datadesconto = datetime.strptime(i['DatadoDesconto'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y'),
        else:
            datadesconto = i['DatadoDesconto']
            
        valordemulta = formatar_valor(i['ValordeMulta'])
        valordejuros = formatar_valor(i['ValordeJuros'])
                
        # SEÇÃO PAGAMENTO
        contadepagamento = i['ContadePagamento']
        agencia = i['Agencia']
        nomebancopagamento = i['NomeBancoPagamento']
        documentodepagamento = i['DocumentodePagamento']
        datadepagamento = datetime.strptime(
            i['DatadePagamento'], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y')
        valorpago = formatar_valor(i['ValorPago'])        
        
        vnumerodotitulo = numerodotitulo
        
        # TRATAR OS CASOS DE FATURAS NÃO NUMERICAS, EX: 247904A PARA SER PELA TABELA DE TITULOS A RECEBER
        # Titulo = 247904A
        # TRATAMENTO COM IF ABAIXO
        
        # TABELA DE ITENS DE FATURAMENTO. AQUI NÃO EXISTEM TÍTULOS COM LETRA EM SEU CÓDIGO
        sql = "select ifat_cdfatura as fatura, ifat_descricao as descricao, ifat_quantidade as quantidade, ifat_precoinf as precounitario, ifat_tpoper as tipooperacao, ifat_noccusto as centrodecusto, ifat_vlrprod as valortotal, ifat_vlrdecr as valordecrescimo, ifat_vlracres as valoracrescimo from MXM_PRD.itfatura_ifat ifat where ifat_cdfatura = :numerodotitulo"
                
        
        if vnumerodotitulo.isdigit():
            # print(str(k) + ' Titulo: ',vnumerodotitulo)
            
            cursor = conexao.cursor()
            cursor = cursor.execute(sql,numerodotitulo = vnumerodotitulo)
            tupla = cursor.fetchall()
                        
            """ elif vnumerodotitulo[0:len(vnumerodotitulo) - 1].isdigit():
                        
            vnumerodotitulo = vnumerodotitulo[0:len(numerodotitulo) - 1]
            #print(str(k) + ' Titulo: ',vnumerodotitulo)
            
            cursor = conexao.cursor()
            cursor = cursor.execute(sql,numerodotitulo = vnumerodotitulo)
            tupla = cursor.fetchall() """
            
        else: # IGNORAR NO FATURAMENTO, TÍTULO DO CONTAS A RECEBER QUE TEM LETRA SEM SER NA ÚLTIMA POSIÇÃO
            continue       
        
        # SEÇÃO APROPRIAÇÃO (PRODUTOS). MAIS DE UMA OCORRÊNCIA POR TÍTULO. PEGANDO DO FATURAMENTO
        
        for g in tupla:                               
                
                valordogrupo = formatar_valor(g[6])
                valordecrescimo = formatar_valor(g[7])
                valoracrescimo = formatar_valor(g[8])
                centrodecusto = consultar_centro_custo(g[5])
                item = g[1]
                quantidade = formatar_quantidade(g[2])
                valorunitario = formatar_valor(g[3])           

                for j in i['InterfaceGrupoPagarReceber']:
                    
                    if j['NumerodoCentrodeCusto'] == centrodecusto:
                        break                    
                    
                codigodogrupo = j['CodigodoGrupo']
                descricaodogrupo = j['DescricaodoGrupo']
                contadofluxodecaixa = j['ContadoFluxodeCaixa']  
                                                                
                # DE CENTRO DE CUSTO PARA BAIXO, PEGAR DO FATURAMENTO  
                #print('K: ',k,' Tamanho da lista tuple: ',len(uftuplelist))
                                                 
                titulo = {
                                'titulo': vnumerodotitulo,
                                'documentofiscal': documentofiscal,
                                'codigocliente': codigoclientefornecedor,
                                'cliente': descricaodoclientefornecedor,
                                'uf' : clicodigodict[codigoclientefornecedor],
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
                                'valordejuros': valordejuros,
                                'contadepagamento': contadepagamento,
                                'agencia': agencia,
                                'valordesconto' : valordesconto,
                                'datadesconto' : datadesconto,
                                'nomebancopagamento': nomebancopagamento,
                                'documentodepagamento': documentodepagamento,
                                'pagamento': datadepagamento,
                                'valorpago': valorpago,
                                'codigodogrupo': codigodogrupo,
                                'grupoderecebimento': descricaodogrupo,
                                'contadofluxodecaixa': contadofluxodecaixa,                    
                                'centrodecusto': centrodecusto,
                                'valorcentrocusto': valordogrupo, # DAQUI PARA BAIXO, ESTÁ PEGANDO DO FATURAMENTO
                                'valordecrescimo' : valordecrescimo,
                                'valoracrescimo' : valoracrescimo,
                                'quantidade' : quantidade,
                                'valorunitario' : valorunitario,
                                'descricaoitem' : item                                                
                        }            

                tituloareceber.append(titulo)                                   
    
    tempo_final = (time())  # em segundos
    print('Tempo jsonparsing: ', tempo_final - tempo_inicial)
            
    return tituloareceber  

def validar_param_entrada(empresa,mes,ano,contadofluxodecaixa):
    
    mesatual = date.today().month
    anoatual = date.today().year
          
    #if int(ano) != anoatual:
    #    raise Anoinvalido    
    
    #elif int(mes) >= mesatual:
    #    raise Mesinvalido
    
    #elif len(empresa) != 3:
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

# MAIN
#
# NumeroTitulo: 252224, Pedido de Faturamento: 262684 

def criar_excel(dftituloareceber):
    
     # Limpando diretório com os arquivos antigos
    remove_arquivos_antigos('consulta receita sng.*')
     
    #nomeplanilha = 'consulta receita sng siseg.xlsx'
    nomeplanilha = f'consulta receita sng_{anopgto}{mespgto}.xlsx'
    
    # 16. Coluna Observação = 16
    #dftituloareceber.insert(loc=1, column='cod_cadu') # 'cadu' -> Nomenclatura da B3
    
    dftituloareceber.rename(columns={
                                        'observacao':'cod_cadu',
                                        'uf' : 'uf_cliente'
                                    },inplace=True)
    
    listacodcadu = []

    for observacao in dftituloareceber['cod_cadu']:  
        if len(re.findall(r'\d+',observacao)) != 0:
            #print('Observacao: ',observacao)               
            result = re.findall(r'\d+',observacao)[0]
            listacodcadu.append(result)
        
        else: # EVITANDO O ERRO Length of values does not match length of index
            listacodcadu.append(None)
    
        
    dftituloareceber['cod_cadu'] = listacodcadu
    
    dftituloarecebercompleto = dftituloareceber
    
    dftituloareceber = dftituloareceber[['titulo','documentofiscal','codigocliente','cliente','uf_cliente','statusdotitulo','empresaemitente','filial','empresarecebedora','pedidofaturamento',
                                        'tipodetitulo','tipodecobranca','emissao','vencimento','competencia','cod_cadu','contadepagamento','nomebancopagamento',
                                        'documentodepagamento','pagamento','valordemulta','valordejuros','codigodogrupo','grupoderecebimento','contadofluxodecaixa','descricaoitem']].drop_duplicates()
    
    dftituloareceber = dftituloareceber.sort_values(by=['competencia','titulo'])        
        
    dftituloareceber.to_excel(nomeplanilha)
    wb = load_workbook(nomeplanilha)
    ws = wb.active
    ws.delete_cols(1)
            
    ajusta_largura(ws)
    wb.save(nomeplanilha)
    
    return nomeplanilha


def remove_arquivos_antigos(padrao):
    files = os.listdir('./')
    for file in files:
        if os.path.isfile(file):
            if len(re.findall(rf"{padrao}",file)) != 0:
                fileremove = re.findall(rf"{padrao}",file)[0]
                print('Arquivo a ser removido: ',fileremove)
                os.remove(f'./{fileremove}')


def cria_dataframe(resultado, cursor):
    
    #print('Resultado: ',resultado)
    df = DataFrame(resultado, columns=[desc[0] for desc in cursor.description])    
    cursor.close()

    #print('Dataframe: ',df)

    return df
    
  
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
        worksheet.column_dimensions[col].width = max_length + 4

    # writer.close()
    
    return worksheet
   


# BANCO HMLG
#senhabd = 'MXM_HMLG#$01'
#usuariobd = 'MXM_HMLG'
#host = 'srv2033.cnseg.int'
#sid = 'HOMOLOG'

# BANCO PRD
senhabd = 'zvga2jd0871'
usuariobd = 'QLIK_MXM'
host = '10.10.20.61'
sid = 'MXMWEB2'

# AMBIENTE HMLG
#BASE_URL = 'https://mxm-hmlg.cnseg.org.br/mxmhom/api'
#loginapi = 
#senhaapi = 
#ambiente = 'WEBMANAGERHOM'

        
# AMBIENTE PRD
BASE_URL = 'https://mxm.cnseg.org.br/Producao/api'
#BASE_URL = 'https://10.10.40.80/Producao/api'

ambiente = 'PRODUCAO'
loginapi = 'INTEGRACAO_MXM'
senhaapi = 'Wd2&9ZL6We4!'

def main(mes,ano):
    
    try:    
                print('Acessando a API...')
                #global ambiente
                #global loginapi
                #global senhaapi
                global contadofluxodecaixa
                global conexao                              
                global mespgto
                global anopgto
                
                mespgto = mes
                anopgto = ano
                
                #if (len(argv)) != 5:
                #    raise ParamErro
                
                #else:
                                
                contadofluxodecaixa = '030101'
                empresa = 'F01'
                            
                """ ano = date.today().year  # ANO ATUAL
                mes = date.today().month # MÊS ATUAL

                mespgto = mes - 1
                anopgto = ano

                if mespgto == 0: # PARA O ANO SEGUINTE
                    mespgto = 12
                    anopgto = ano - 1 """
                
                # CASO PRECISE FORÇAR A CARGA DE DADOS. UTILIZAR NÚMEROS INTEIROS
                #anopgto = 2024
                #mespgto = 9        

                mespgto = str(mespgto).zfill(2)
                anopgto = str(anopgto) 

                #print('Mes pagamento: ',mespgto)
                #print('Ano pagamento: ',anopgto)

                #validar_param_entrada(empresa = argv[1], mes = argv[2], ano = argv[3], contadofluxodecaixa = argv[4])

                validar_param_entrada(empresa = empresa, mes = mespgto, ano = anopgto, contadofluxodecaixa = contadofluxodecaixa)

                conexao = conexao_bd(senhabd, usuariobd, host, sid) 
                
                json = consultar_titulo_receber(empresa = empresa, mes = mespgto, ano = anopgto) # OBTENDO JSON.
                
                # ÚLTIMA VALIDAÇÃO, APÓS A OBTENÇÃO DO JSON
                try:
                    json['Message']                
                    
                except KeyError as e: # NAO TEM A CHAVE NO DICIONÁRIO, ENTÃO TEM QUE TER A CHAVE 'Messages' PROSSEGUE EXECUÇÃO
                    if len(json['Messages']) != 0:
                        raise ErroAPI
                    
                else: # VERIFICA SE TEM A CHAVE 'Message' no DICIONÁRIO. LANÇAR A EXCEÇÃO
                    if len(json['Message']) != 0:
                        raise ErroGeralAPI
                                                                    

    except EmpErro:
            print("O código da empresa deve possuir 3 caracteres")
            
    except ParamErro:
            print("Número incorreto de parâmetros. Devem ser 4 Empresa Mêspagamento Anopagamento ContaFLuxodeCaixa (Ex:python.exe consulta_titulo_receber.py F01 01 2024 030101) ")

    except FlxErro:
            print("A conta do fluxo de caixa deve ter pelo menos 6 dígitos numéricos")   

    except MesErro:
            print("O mês deve ter dois dígitos numéricos")

    except AnoErro:
            print("O ano deve ter quatro dígitos numéricos")

    except ErroAPI:
            print('Erro da API: ', json['Messages'][0]['Message'])

    except ErroGeralAPI:
            print('Erro Geral API: ', json['Message'])
            
    except Mesinvalido:
            print("Mes de pagamento inválido. Mês ainda não terminado")
            
    except Anoinvalido:
            print("Ano inválido. O ano informado deve ser o atual")

    except oracledb.OperationalError as e:
            print('Erro de conexão ao Oracle')

    # SE NÃO TIVER NENHUM ERRO, VEM PARA CÁ
    else:
        
        try:
            
            global centrosdecusto                        
            
            centrosdecusto = {} # DICIONÁRIO DE CENTROS DE CUSTO
            tituloareceber = jsonparsing(json) # O RETORNO É UMA LISTA DE DICTIONÁRIOS
            dftituloareceber = criar_dataframe(tituloareceber)  
            
            planilhareceitasng = criar_excel(dftituloareceber) # FORMATANDO O DATAFRAME. CRIADA MENSALMENTE COM DADOS DO MÊS ANTERIOR        
                    
            conexao.close()
        
            tempo_final = (time()) # em segundos
        
            print(f"Tempo Total {tempo_final - tempo_inicial} segundos")
            
            return planilhareceitasng
                
        except NotitErro:
            print("Sem títulos para os argumentos passados")
            
        
#if __name__ == "__main__":        
#    main()