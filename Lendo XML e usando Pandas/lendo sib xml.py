from bs4 import BeautifulSoup
import pandas as pd
import os
import re

def create_list(beneficiarios):
    for beneficiario in beneficiarios:
        nome.append(beneficiario.nome.get_text())

        if beneficiario.cpf is None:
            cpf.append("")
        else:
            cpf.append(beneficiario.cpf.get_text())

        codigobeneficiario.append(beneficiario.codigobeneficiario.get_text())

        cco.append(beneficiario.attrs['cco'])
        situacao.append(beneficiario.attrs['situacao'])
        data_atualizacao.append(beneficiario.attrs['dataatualizacao'])

        data = {'cco':cco , 'nome':nome, 'cpf':cpf, 'codigobeneficiario':codigobeneficiario, 'situacao':situacao,   'dataatualizacao':data_atualizacao}

    return data    

def create_excel(data):
    df = pd.DataFrame(data)
    df_sorted = df.sort_values(by='nome')    

    #print(df_sorted)

    excel = df_sorted.to_excel('Arquivo SIB.xlsx')    

    return excel

#xml_path = 'C:\\Users\\antoniodantas\\Treinamento Cientista de Dados\\Treinamento-Cientista-de-Dados\\Lendo XML e usando Pandas\\'

# EM CASA
#xml_path = 'C:\\Users\\anton\\OneDrive\\Documentos\\Treinamento Cientista de Dados\\Lendo XML e usando Pandas\\'


#xml_file = open(xml_path + 'ArqConf3139040220210101.CNX.XML')

files = os.listdir()


print(re.search(r'.+\.xml', files))

'''
for x in files:
    result = re.search(r'.+\.xml',x)
    
    if result is not None:
        xml_file = path + "\" + result.group(0)
        print('Arquivo XML: ',xml_file)
        
        content = open(xml_file.read())


cco = []
nome = []
cpf = []
codigobeneficiario = []
situacao = []
data_atualizacao = []

lista = []

bs = BeautifulSoup(content,'lxml')
#print(bs)

beneficiarios = bs.find_all('beneficiario')

print(create_excel(create_list(beneficiarios)))
'''