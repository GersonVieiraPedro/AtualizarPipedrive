from datetime import datetime
import pandas as pd



def criar_dataframe_anomes(dataInicial, dataFinal):

    # Cria um intervalo de datas
    datas = pd.date_range(start=dataInicial, end=dataFinal, freq='MS').strftime('%Y%m').astype(int)
    # Cria um DataFrame com as datas
    df = pd.DataFrame(datas, columns=['Data'])

    print(df)
    return df


def criar_dataframe_temporal (dataframeData = pd.DataFrame(), dataframeGrupos = pd.DataFrame()):
    dataframe = dataframeData.merge(dataframeGrupos, how='cross')
    return dataframe

def criar_dataframe_RDM(dataframeData = pd.DataFrame(), dataframeGrupos = pd.DataFrame(), dataframefuncionario = pd.DataFrame(), dataframeValor = pd.DataFrame()):
    """
    Função para criar um DataFrame RDM (Recência, Frequência, Valor Monetário) a partir de dados de grupos econômicos e funcionários.
    Parâmetros:
    - dataframeData: DataFrame contendo as datas e grupos econômicos.
    - dataframeGrupos: DataFrame contendo os grupos econômicos.
    - dataframefuncionario: DataFrame contendo os dados dos funcionários.
    - dataframeValor: DataFrame contendo os dados de valor monetário e último faturamento por grupo.
    Retorna:
    - DataFrame com a quantidade de funcionários ativos por grupo econômico e mês, incluindo MoM (Month over Month) e variação percentual.
    """



    # Faz join entre a tabela de grupos e a de funcionários, associando cada cliente ao seu grupo econômico
    dataframe = dataframeGrupos.set_index('CodigoCliente').join(
        dataframefuncionario.set_index("COD Contrato G.I"),
        on="CodigoCliente"
    )

    # Converte colunas de datas para string no formato YYYYMM
    dataframe['AnoMesAdmissao'] = dataframe['AnoMesAdmissao'].astype(str)
    dataframe['AnoMesDemissao'] = dataframe['AnoMesDemissao'].fillna('999912').astype(str)  # 999912 representa "sem demissão"
    dataframeData['Data'] = dataframeData['Data'].astype(str)  # Data do calendário também vira string YYYYMM

    # Agrupa os dados para obter a quantidade total de funcionários por grupo, admissão e demissão
    dataframe = dataframe.pivot_table(
        index=['Grupo Econômico', 'AnoMesAdmissao', 'AnoMesDemissao'],
        values='QtdeFuncionario',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Exporta o dataframe intermediário para CSV para depuração
    dataframe.to_csv("DataFrame.csv", encoding="utf-8-sig")
    
    # Realiza o join da base temporal (data + grupos) com os dados de funcionários por grupo
    # Isso cria todas as combinações possíveis entre grupo econômico e AnoMes do calendário
    base = dataframeData.merge(dataframe, on="Grupo Econômico", how="left")

    # Exporta base para depuração
    base.to_csv("Base.csv", encoding="utf-8-sig")

    # Filtra os registros onde o funcionário estava ativo no mês da linha (Data está entre admissão e demissão)
    df_filtered = base[
        (base['AnoMesAdmissao'] <= base['Data']) &
        ((base['AnoMesDemissao'] >= base['Data']) | (base['AnoMesDemissao'].isna()))
    ]

    # Agrupa os dados finais por grupo e mês, somando a quantidade de funcionários ativos
    resultado = df_filtered.groupby(['Grupo Econômico', 'Data'])['QtdeFuncionario'].sum().reset_index()

    # Renomeia a coluna para "Ativos"
    resultado.rename(columns={'QtdeFuncionario': 'Ativos'}, inplace=True)

    # Pegando o resultado do mês anterior para calcular MoM (Month over Month)
    resultado['AtivosMoM'] = (
        resultado
        .sort_values(['Grupo Econômico', 'Data'])  # Ordena para garantir sequência
        .groupby('Grupo Econômico')['Ativos']  # Agrupa por grupo econômico
        .shift(1)  # Pega o valor anterior (mês anterior dentro do grupo)
    )

    # Calcula a variação percentual MoM DEMANDA
    resultado['Demanda'] = (
        (resultado['Ativos'] - resultado['AtivosMoM']) / resultado['AtivosMoM']
    ).fillna(0) # Preenche NaN com 0 para meses sem dados

    resultado = resultado[resultado['Demanda'] != 0]  # Remove linhas onde a demanda é zero

    # Converte para porcentagem e arredonda
    resultado['%'] = resultado['Demanda'].apply(lambda x: round(x * 100, 2)) 

    # Exportando a tabela por mes e grupo econômico sem agrupar o resultado
    resultado.to_csv("Demanda_Mensal.csv", encoding="utf-8-sig")
    
    # Calcula a média de demanda por grupo econômico
    dataframeDemanda = resultado.groupby('Grupo Econômico', as_index=False)['Demanda'].mean()
    dataframeDemanda.rename(columns={'Demanda': 'Media_Demanda'}, inplace=True)

    #Pegando todos os grupos econômicos ativos e unicos 
    Grupos = dataframeData[['Grupo Econômico']].drop_duplicates()
 
    # Faz o merge com o demanda final, preservando todos os grupos mesmo os que tiveram funcionarios ativos no período
    demanda_completo = Grupos.merge(dataframeDemanda, on='Grupo Econômico', how='left')

    # Exporta o demanda completo para CSV
    demanda_completo.to_csv("Demanda_Final.csv", encoding="utf-8-sig")

    # Converte as colunas de data para o formato datetime e depois para o formato de data
    dataframeValor['UltimoFat'] = pd.to_datetime(dataframeValor['UltimoFat'], errors='coerce') 
    
    dataframeValor['PrimeiroFat'] = pd.to_datetime(dataframeValor['PrimeiroFat'], errors='coerce')
    dataframeValor['PrimeiroFat'] = dataframeValor['PrimeiroFat'].dt.date

    # Calculando a diferença de dias entre a data atual e o último faturamento
    dataframeValor['DiasUltimoFat'] = (pd.to_datetime(datetime.now()) - dataframeValor['UltimoFat']).dt.days

    dataframeFiltrado = dataframeValor[['Bruto', 'UltimoFat', 'PrimeiroFat', 'DiasUltimoFat']]

    data = demanda_completo.set_index('Grupo Econômico').join(dataframeFiltrado, on='Grupo Econômico', how='left')

    data.to_csv("RDM.csv", encoding="utf-8-sig")






""" 
LEMBRETE


Feito:
Demanda por grupo econômico
Valor Bruto por grupo econômico (já estava feito)

Precisa fazer :
Recência por grupo econômico ( Dias da última fatura do grupo econômico)
Classificação RDM por grupo econômico (Recência, Frequência, Valor Monetário)



"""