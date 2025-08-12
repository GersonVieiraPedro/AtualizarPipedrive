

import pandas as pd

import numpy as np
from datetime import date, timedelta, datetime
import requests
import logging
import time
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ConsultandoDados import consultandoSQL, consultandoAzure
from Email import EnviarEmail
from RDM import criar_dataframe_RDM, criar_dataframe_anomes, criar_dataframe_temporal
from auxiliar import criar_pastas, get_arquivo, salvar_historico
load_dotenv()
criar_pastas()


# Obtenção da data atual
DataAtual = date.today()

# Cálculo do início do intervalo (365 dias atrás)
DataCorte = DataAtual - timedelta(days=365)

""" Mudamos os parâmetros de DataInicio e DataFinal'

# Ajuste para o primeiro dia do mês de DataInicio
DataInicio = DataInicio - timedelta(days=(DataInicio.day - 1))

# Determinando o último dia do mês passado
DataFinalMes = DataAtual.month - 1
DataFinalAno = DataAtual.year

# Ajuste quando o mês for janeiro (mês 1), para o último mês do ano anterior
if DataFinalMes == 0:
    DataFinalMes = 12
    DataFinalAno -= 1

# Encontrando o último dia do mês
DataFinalDia = calendar.monthrange(DataFinalAno, DataFinalMes)[1]
DataFinal = date(DataFinalAno, DataFinalMes, DataFinalDia)
"""
# Formatando as datas como string
DataFinal = DataAtual.strftime("%Y-%d-%m")
DataInicio = DataCorte.strftime("%Y-%d-%m")

# Exibindo as datas para conferência
print(DataInicio)
print(DataFinal)



# Consultando dados no banco de dados SQL Server
TB_ValorAgrupado, TB_DataFat, TB_Funcionario = consultandoSQL(DataInicio, DataFinal)


# Consultando dados no Azure Data Lake
TB_Organizacao_Completo = consultandoAzure()

# Criando um array de COD GI na coluna COD GI
TB_Organizacao_Completo["COD Contrato G.I"] = TB_Organizacao_Completo["COD Contrato G.I"].str.split(".")

# Expandindo os dados para cada valor de COD GI
df_Explodido = TB_Organizacao_Completo.explode("COD Contrato G.I")

# Selecionando as colunas necessárias
TB_Organizacao = df_Explodido[["COD Contrato G.I", "Grupo Econômico"]]

# Removendo valores nulos na coluna "COD Contrato G.I"
TB_Organizacao = TB_Organizacao[TB_Organizacao["COD Contrato G.I"].notna()]

# Realizando um join entre os DataFrames
TB_ValorAgrupado = TB_ValorAgrupado.set_index("CodigoCliente").join(TB_Organizacao.set_index("COD Contrato G.I"), on="CodigoCliente")

# Criando a tabela de faturamento total por grupo econômico
TB_TotalFaturamento = pd.pivot_table(TB_ValorAgrupado, values=["Bruto"], index=["Grupo Econômico"], aggfunc={"Bruto": "sum"}, fill_value=0)

#Ordenando o valor bruto em Decrecente
TB_TotalFaturamento = TB_TotalFaturamento.sort_values(by="Bruto", ascending=False)

#Acumulando o valor bruto
TB_TotalFaturamento["Faturamento Acumulado"] = TB_TotalFaturamento["Bruto"].cumsum()

# Calculando a porcentagem acumulada
TB_TotalFaturamento["Porcentagem Acumulado"] = (TB_TotalFaturamento["Faturamento Acumulado"] / TB_TotalFaturamento["Bruto"].sum())

# Classificação ABC
Criterios = [
    (TB_TotalFaturamento["Porcentagem Acumulado"] <= 0.80),
    (TB_TotalFaturamento["Porcentagem Acumulado"] > 0.80) & (TB_TotalFaturamento["Porcentagem Acumulado"] <= 0.95),
    (TB_TotalFaturamento["Porcentagem Acumulado"] > 0.95)
]

# Classificação ABC : A = 27, B = 28, C = 29 (Está em Numero que é o ID dessa opção no Pipedrive)
resultados = [27, 28, 29]

#Criando a coluna de classficiação avançada
TB_TotalFaturamento["Curva ABC"] = np.select(Criterios, resultados, default=694)

TB_TotalFaturamento["Status Grupo"] = 654

# Salvando o DataFrame em um arquivo CSV
TB_TotalFaturamento.to_csv("Historico/TB_TotalFaturamento.csv")

"""
RDM
TB_AnoMes = criar_dataframe_anomes(DataCorte, DataAtual)

TB_GruposAtivos = pd.DataFrame(TB_TotalFaturamento.index, columns=["Grupo Econômico"])
print(TB_GruposAtivos)

TB_Temporal = criar_dataframe_temporal(TB_AnoMes, TB_GruposAtivos)

TB_Temporal.to_csv("Historico/TB_Temporal.csv", index=False)
criar_dataframe_RDM(TB_Temporal, TB_Funcionario, TB_Organizacao, TB_TotalFaturamento)

"""

#Filtrando o dataframe para trazer somente as colunas necessárias
TB_Final_Distinct = TB_Organizacao_Completo[["id", "COD Contrato G.I", "Grupo Econômico"]]

#Realizando um Join para trazer a curva por Grupo Econômico
TB_Final_Distinct = TB_Final_Distinct.set_index("Grupo Econômico").join(TB_TotalFaturamento, on="Grupo Econômico")

# Garante que valores NaN após o join sejam tratados
TB_Final_Distinct["Curva ABC"] = TB_Final_Distinct["Curva ABC"].fillna(694).astype(int)

# Garante que valores NaN após o join sejam tratados
TB_Final_Distinct["Status Grupo"] = TB_Final_Distinct["Status Grupo"].fillna(700).astype(int)

# Exportando o DataFrame para um arquivo CSV
TB_Final_Distinct.to_csv("Historico/TB_Final_Distinct.csv")

#Explodindo os valores de COD Contrato G.I
TB_Final_Explode = TB_Final_Distinct.explode("COD Contrato G.I")

TB_Final_Explode.to_csv("Historico/TB_Final_Explode.csv")

# Realizando join para obter as datas de primeiro e último faturamento
TB_DataFat = pd.pivot_table(TB_DataFat, values=["PrimeiroFat", "UltimoFat"], index=["CodigoCliente"], aggfunc={"PrimeiroFat":"min", "UltimoFat":"max"}, fill_value=0)
TB_Final_Explode = TB_Final_Explode.set_index("COD Contrato G.I").join(TB_DataFat[["PrimeiroFat", "UltimoFat"]], on="COD Contrato G.I" , rsuffix='_r')

# Convertendo para datetime
TB_Final_Explode["PrimeiroFat"] = pd.to_datetime(TB_Final_Explode["PrimeiroFat"], errors="coerce")
TB_Final_Explode["UltimoFat"] = pd.to_datetime(TB_Final_Explode["UltimoFat"], errors="coerce")

# Pivotando os dados por "id"
TB_Final_Explode = pd.pivot_table(TB_Final_Explode, values=["PrimeiroFat", "UltimoFat"], index=["id"], aggfunc={"PrimeiroFat":"min", "UltimoFat":"max"}, fill_value=0)

# Filtrando as colunas necessárias
TB_Final_Distinct = TB_Final_Distinct[["id", "Curva ABC", "Status Grupo"]]

# Realizando o join final para unir as tabelas
TB_Final = TB_Final_Distinct.set_index("id").join(TB_Final_Explode, on="id")

#  dropna para remover linhas onde todas as colunas são NaN
TB_Final = TB_Final.dropna(how="all")

# Calculando a diferença de dias do último faturamento
TB_Final["DiasUltimoFat"] = (datetime.now() - TB_Final["UltimoFat"]).dt.days


TB_Organ_Final = TB_Organizacao_Completo[[
    "id",
    "Categoria",
    "Primeiro Faturamento",
    "Último Faturamento", 
    "Status da Organização",
    "Grupo Econômico",
    "8cc8862b52157830bf56ee015fcedcca7ea75dba"
    ]].set_index("id")

TB_Organ_Final = TB_Organ_Final.rename(columns={"8cc8862b52157830bf56ee015fcedcca7ea75dba": "Status Grupo Econômico"})

TB_Organ_Final = TB_Organ_Final.join(TB_Final, on="id")

# Convertendo para datetime64 primeiro
TB_Organ_Final["Primeiro Faturamento"] = pd.to_datetime(TB_Organ_Final["Primeiro Faturamento"], errors="coerce")

TB_Organ_Final["Primeiro Faturamento"] = TB_Organ_Final["Primeiro Faturamento"].dt.date


# Convertendo para datetime64 primeiro
TB_Organ_Final["PrimeiroFat"] = pd.to_datetime(TB_Organ_Final["PrimeiroFat"], errors="coerce")


TB_Organ_Final["PrimeiroFat"] = TB_Organ_Final["PrimeiroFat"].dt.date


TB_Organ_Final["MudouPrimeiroFat?"] = TB_Organ_Final["PrimeiroFat"] != TB_Organ_Final["Primeiro Faturamento"]

TB_Organ_Final["MudouUltimoFat?"] = TB_Organ_Final["UltimoFat"] != TB_Organ_Final["Último Faturamento"]

TB_Organ_Final["MudouFat?"] = np.where(TB_Organ_Final["UltimoFat"].isna(), np.nan ,
    np.where(
        (TB_Organ_Final["MudouUltimoFat?"] == True) | (TB_Organ_Final["MudouUltimoFat?"] == True), 
        True, 
        False 
    )
)

# Definindo o status da organização de acordo com os dias do último faturamento
TB_Organ_Final["Status"] = np.where(TB_Organ_Final["DiasUltimoFat"].isna(), np.nan, 
    np.where(TB_Organ_Final["DiasUltimoFat"] <= 365, 
        171, 
        172
    )
)

# Definindo a coluna de MudouStatus
TB_Organ_Final["MudouStatus?"] = np.where(TB_Organ_Final["DiasUltimoFat"].isna(), np.nan, 
    np.where(TB_Organ_Final["Status da Organização"] != TB_Organ_Final["Status"] ,
    1,
    0
    ) 
)

# Definindo a coluna de Curva ABC 
TB_Organ_Final["MudouCurva?"] = np.where(TB_Organ_Final["Curva ABC"].isna(), np.nan,
    np.where(
        (TB_Organ_Final["Categoria"].isna()) & (TB_Organ_Final["Curva ABC"] == 694),
        0,
        np.where(
            TB_Organ_Final["Categoria"] != TB_Organ_Final["Curva ABC"],
            1,
            0
        )
    )
)

# Estou fazendo essa segunda verificação para incluir os inativos já que os que não estavam ativos foram preenchidos com 700 "-"
# Os ativos foram incluidos com todos os grupos econômicos que tinham faturamento nos 12 meses de corte lá no começo
TB_Organ_Final["Status Grupo"] = np.where(
    (TB_Organ_Final["DiasUltimoFat"] > 365) & (TB_Organ_Final["Status Grupo"] == 700),
    655,
    TB_Organ_Final["Status Grupo"]
)


# Depois que verificamos se o grupo econômico por ID está ativo, inativo ou nunca foi ativo. precisamos aplicar a regra para todas as organizações
TB_Organ_Final["Status Grupo"] = (
    TB_Organ_Final.groupby("Grupo Econômico")["Status Grupo"]
    .transform(lambda x: 654 if (x == 654).any() else (655 if (x == 655).any() else 700))
)

# Verificando se o grupo economico mudou e não estava vazio e for modificado para 700 "-"
TB_Organ_Final["MudouStatusGE?"] = np.where( 
    (TB_Organ_Final["Status Grupo Econômico"].isna()) & ( TB_Organ_Final["Status Grupo"] == 700),
    0,
    np.where(
        TB_Organ_Final["Status Grupo Econômico"] != TB_Organ_Final["Status Grupo"],
        1,
        0
    )
)

#Exportando o DataFrame final para um arquivo CSV para fazer validações
TB_Organ_Final.to_csv("Historico/TB_Organ_Final.csv")

# Filtrando os dados para obter apenas as organizações que tiveram alguma mudança
TB_Final = TB_Organ_Final[( TB_Organ_Final["MudouFat?"] == 1 ) | ( TB_Organ_Final["MudouStatus?"] == 1) | (TB_Organ_Final["MudouCurva?"] == 1) | (TB_Organ_Final["MudouStatusGE?"] == 1)]

#Exportando o DataFrame final para um arquivo CSV para fazer validações
TB_Final.to_csv("Historico/TB_Final.csv")

TB_ResumoCurva = TB_Final.groupby("Curva ABC").agg(
    Empresas = ("Curva ABC", "count"),
    Grupos = ("Grupo Econômico", "nunique")
)

TB_ResumoCurva.index = TB_ResumoCurva.rename(index={27: "A", 28: "B", 29: "C"}).index



TB_GruposMudaram = TB_Final[TB_Final["MudouCurva?"] == 1].groupby("Grupo Econômico").agg(
    Antigo = ("Categoria", "min"),
    Atual = ("Curva ABC", "min")
)



API_TOKEN = os.getenv("API_TOKEN")
#API_TOKEN = API_TOKEN.replace(",","")

DfStatus = pd.DataFrame({"ID_Organizacao":[],"Status":[], "Cod_HTTP":[]})

# Função para enviar os dados via API
def AtualizaSistema(id: int, status: int = None, curva: int = None, PrimeiroFat: date = None, UltimoFat: date = None, StatusGE: int = None):
    try:
        url = f"https://api.pipedrive.com/v1/organizations/{id}?api_token={API_TOKEN}"

        dados = {}
        
        # Adicionando dados ao payload conforme necessário
        if status is not None and not pd.isna(status):
            dados.update({"64506c623805f8b073a60555309a4bf2d0949f76": round(status)})
        if curva is not None and not pd.isna(curva):
            dados.update({"44cc10af09a12836d1843f5f5657484de7bc71d6": round(curva)})
        if PrimeiroFat is not None and not pd.isna(PrimeiroFat):
            dados.update({"3c324f964624bdc71b66ff21c0cfdf097dc39836": str(PrimeiroFat)})
        if UltimoFat is not None and not pd.isna(UltimoFat):
            dados.update({"fbe5a67d1d2bc41850c675402c2456ac50780dad": str(UltimoFat)})
        if StatusGE is not None and not pd.isna(StatusGE):
            dados.update({"8cc8862b52157830bf56ee015fcedcca7ea75dba": round(StatusGE)})

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Enviando a requisição PUT
        response = requests.put(url, headers=headers, json=dados)

        # Logando o sucesso ou falha da requisição
        if response.status_code == 200:
            logging.info(f"id: {id}, status: Sucesso, cod_http: {response.status_code}")
            DfStatus.loc[len(DfStatus)] = [id, "Sucesso", response.status_code]
        else:
            logging.warning(f"id: {id}, status: Falha, cod_http: {response.status_code}")
            DfStatus.loc[len(DfStatus)] = [id, "Falha", response.status_code]

        return response
    except Exception as e:
        logging.error(f"id: {id}, status: Erro, cod_http: {response.status_code}, erro:{e}")
        DfStatus.loc[len(DfStatus)] = [id, "Erro", response.status_code]
        raise

# Iterando sobre os dados para atualizar via API
Atualzacao = []

for index, row in TB_Final.iterrows():

    # Função auxiliar para tratar NaN antes do round
    def safe_round(value):
        return round(value) if pd.notna(value) else 0

    # Verificando se houve mudança dos dados
    Status = row["Status"] if row.get("MudouStatus?") == 1 else None
    Curva = row["Curva ABC"] if row.get("MudouCurva?") == 1 else None
    PrimeiroFat = row["PrimeiroFat"] if row.get("MudouPrimeiroFat?") == True and row.get("MudouFat?") == 1 else None
    UltimoFat = row["UltimoFat"] if row.get("MudouUltimoFat?") == True and row.get("MudouFat?") == 1 else None
    StatusGE = row["Status Grupo"] if row.get("MudouStatusGE?") == 1 else None

    Atualzacao.append({
        "id": index,
        "Mudou": f"{safe_round(row['MudouStatus?'])}.{safe_round(row['MudouCurva?'])}.{(1 if row['MudouPrimeiroFat?'] == True and row['MudouFat?'] == 1 else 0)}.{(1 if row['MudouUltimoFat?'] == True and row['MudouFat?'] == 1 else 0)}.{safe_round(row['MudouStatusGE?'])}",
        "status": Status,
        "curva": Curva,
        "PrimeiroFat": PrimeiroFat,
        "UltimoFat": UltimoFat,
        "StatusGE": StatusGE
    })

    #if index % 10 == 0:
        # Aguardar 2 segundos a cada 10 iterações para evitar sobrecarga na API
        #break #testar os 10 primeiros
        #print("Aguardando 2 segundos para evitar sobrecarga na API...")
        #time.sleep(2)

Atualzacao = pd.DataFrame(Atualzacao)
Atualzacao.to_csv("Historico/AtualizaçãoResumo.csv")

salvar_historico(TB_Final)


EnviarEmail(DfStatus, TB_GruposMudaram, TB_ResumoCurva)


#except Exception as e:
#    import traceback
#    traceback.print_exc()  # Exibe o erro completo
#    input("\nErro encontrado. Pressione Enter para sair...")
#else:
#    input("\nExecução concluída. Pressione Enter para sair...")