
import pyodbc
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO
import os

from dotenv import load_dotenv 
load_dotenv()




#Usando o Dotenv para carregar as variáveis de ambiente e manter a segurança delas
DATABASE = os.getenv("DATABASE")
DRIVER = os.getenv("DRIVER")
SERVER = os.getenv("SERVER")
UID = os.getenv("USER")
PWD = os.getenv("PASSWORD")


def consultandoSQL(DataInicio, DataFinal):
    """
    Função para consultar dados no banco de dados SQL Server e retornar DataFrames com os resultados.
    
    Parâmetros:
    - DataInicio: Data inicial para a consulta (formato 'YYYY-MM-DD').
    - DataFinal: Data final para a consulta (formato 'YYYY-MM-DD').
    
    Retorna:
    - TB_ValorAgrupado: DataFrame com valores agrupados por cliente e mês.
    - TB_DataFat: DataFrame com o primeiro e último faturamento de cada cliente.
    - TB_Funcionario: DataFrame com informações de funcionários agrupados por cliente e mês.
    """
    StrCnxn = "DRIVER={ODBC Driver 17 for SQL Server};"+f"""SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}"""

    #print(StrCnxn)

    # Estabelecendo conexão com o banco de dados utilizando ODBC
    cnxn = pyodbc.connect(StrCnxn)

    # Consulta para buscar valores agrupados por cliente e mês
    sql = f"""
        SELECT 
            CodigoCliente,
            FORMAT(DataEmissao, 'yyyyMM') AS AnoMes,  
            SUM(ValorBruto) AS Bruto,
            FORMAT(MAX(DataEmissao), 'yyyyMMdd') AS UltimoFat,
            FORMAT(MIN(DataEmissao), 'yyyyMMdd') AS PrimeiroFat
        FROM TB_Duplicata
        WHERE 
            [Status] <> 'C' AND 
            NroNfe <> 0 AND
            DataEmissao >= '{DataInicio}' AND 
            DataEmissao <= '{DataFinal}'
        GROUP BY CodigoCliente, FORMAT(DataEmissao, 'yyyyMM')
        ORDER BY Bruto DESC
    """
    Tabela = pd.read_sql(sql, cnxn, dtype={"CodigoCliente": str})

    # Criando DataFrame com os dados obtidos
    TB_ValorAgrupado = pd.DataFrame(Tabela)

    # Segunda consulta para obter o primeiro e último faturamento de cada cliente
    sql = f"""
        SELECT 
            CodigoCliente,
            FORMAT(MAX(DataEmissao), 'yyyyMMdd') AS UltimoFat,
            FORMAT(MIN(DataEmissao), 'yyyyMMdd') AS PrimeiroFat
        FROM TB_Duplicata
        WHERE 
            [Status] <> 'C' 
        GROUP BY CodigoCliente
    """
    TB_DataFat = pd.read_sql(sql, cnxn, dtype={"CodigoCliente": str})
    TB_DataFat = pd.DataFrame(TB_DataFat)


    sql = f"""
        SELECT 
            CodigoCliente,
            FORMAT(DataAdmissao,'yyyyMM') AS AnoMesAdmissao ,
            FORMAT(DataDemissao,'yyyyMM') AS AnoMesDemissao,
            COUNT(CodigoFuncionario) AS QtdeFuncionario
        FROM TB_Funcionario 
        GROUP BY 
            CodigoCliente, 
            FORMAT(DataAdmissao,'yyyyMM'),
            FORMAT(DataDemissao,'yyyyMM')
    """

    Tabela = pd.read_sql(sql, cnxn, dtype={"CodigoCliente": str})
    TB_Funcionario = pd.DataFrame(Tabela)

    # Fechando a conexão com o banco de dados
    cnxn.close()

    # Retornando os DataFrames com os resultados
    return TB_ValorAgrupado, TB_DataFat, TB_Funcionario



def consultandoAzure():
    """
    Função para consultar dados no Azure Data Lake e retornar um DataFrame com os resultados.
    """
    account_url = os.getenv("ACCOUNT_URL")
    container_name = os.getenv("CONTAINER")
    file_path = os.getenv("FILE")
    account_key = os.getenv("KEY")

    #print(f"""{account_url}  {container_name}  {file_path}  {account_key}""")

    # Criando o cliente do serviço Data Lake
    service_client = DataLakeServiceClient(account_url=account_url, credential=account_key)
    file_system_client = service_client.get_file_system_client(container_name)

    # Baixando o arquivo CSV
    file_client = file_system_client.get_file_client(file_path)
    downloaded_file = file_client.download_file()

    # Lendo o conteúdo do arquivo CSV em um DataFrame
    csv_data = downloaded_file.readall().decode("utf-8")
    df = pd.read_csv(StringIO(csv_data), delimiter="|", dtype={"COD Contrato G.I": str})

    # Preparando e transformando dados
    TB_Organizacao_Completo = pd.DataFrame(df)

    return TB_Organizacao_Completo
