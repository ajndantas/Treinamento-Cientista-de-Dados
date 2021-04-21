from bs4 import BeautifulSoup
import pandas as pd

def create_cd_plano(lista):

    cd_plano = []

    # UMA MANEIRA DE FAZER SWITCH: https://data-flair.training/blogs/python-switch-case/

    plano = {
                '467928128':'10',
                '467927120':'20',
                '467577121':'40',
                '474938153':'65',
                '477410168':'35',
                '467578129':'30 e 70',
                '484059193':'61',
                '467576122':'60',
             }
    
    for numeroplanoans in lista:
        cd_plano.append(plano.get(numeroplanoans,""))
 
    return cd_plano

def create_list(beneficiarios):

    # Colunas da planilha
    cco = []
    nome = []
    codigobeneficiario = []
    situacao = []
    data_atualizacao = []
    cpf = []
    numeroplanoans = []    

    for beneficiario in beneficiarios:
        
        cco.append(beneficiario.attrs['cco'])
        nome.append(beneficiario.nome.get_text())
        codigobeneficiario.append(beneficiario.codigobeneficiario.get_text())       
        situacao.append(beneficiario.attrs['situacao'])
        data_atualizacao.append(beneficiario.attrs['dataatualizacao'])        

        if beneficiario.cpf is None:
            cpf.append("")
        else:
            cpf.append(beneficiario.cpf.get_text())

        if beneficiario.numeroplanoans is None:
            numeroplanoans.append("")
        else:
            numeroplanoans.append(beneficiario.numeroplanoans.get_text())

        data = {'cco':cco , 'nome':nome, 'cpf':cpf, 'codigobeneficiario':codigobeneficiario, 'situacao':situacao, 'numeroplanoans':numeroplanoans, 'dataatualizacao':data_atualizacao}

    return data    
   
xml_file = open('ArqConf3139040220210101.CNX.xml')

bs = BeautifulSoup(xml_file.read(),'lxml')

# Tag que possui as colunas da planilha
beneficiarios = bs.find_all('beneficiario')

lista = create_list(beneficiarios)

df = pd.DataFrame(lista)

# Adicionando esta coluna ao dataframe
df.insert(loc=4,column='cd_plano',value=create_cd_plano(lista['numeroplanoans']),allow_duplicates=True)

df_sorted = df.sort_values(by=['nome','cd_plano'])
df_sorted.to_excel('Arquivo SIB.xlsx')
