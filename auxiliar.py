from datetime import datetime
import os
import sys

import pandas as pd

def criar_pastas():
    # Caminho da pasta
    pasta_historico = os.path.join(os.getcwd(), "Historico")
    pasta_Atualizacao = os.path.join(os.getcwd(), "Atualizacao")
    # Tenta criar a pasta
    try:
        os.makedirs(pasta_historico, exist_ok=True)
        os.makedirs(pasta_Atualizacao, exist_ok=True)
        print(f"Pasta criada ou já existente: {pasta_historico} e {pasta_Atualizacao}")
    except Exception as e:
        print(f"Erro ao criar a pasta: {e}")


def get_arquivo(filename):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath("."), filename)



def salvar_historico(TB_Final):
    # Salvando dados históricos de atualização
    TB_Final["DataAtualizacao"] = datetime.now()
    CaminhoArquivo = "Historico/TB_HistoricoAtualizacao.csv"

    # Garante que ID_Organizacao seja coluna, não índice
    TB_Final = TB_Final.reset_index(names="id_Organizacao")

    if not os.path.exists(CaminhoArquivo):
        TB_Final.to_csv(CaminhoArquivo, index=True)  # index=True cria o índice sequencial
    else:    
        TB_Historico = pd.read_csv(CaminhoArquivo, index_col=0)  # lê usando a primeira coluna como índice
        TB_Historico = pd.concat([TB_Historico, TB_Final], ignore_index=True)
        TB_Historico.to_csv(CaminhoArquivo, index=True)



