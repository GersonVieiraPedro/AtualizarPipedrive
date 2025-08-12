# 🚀 Automação de Atualização de dados no Pipedrive

Este projeto é um script Python focado em **automação de coleta, análise e atualização** de dados corporativos, integrando bancos SQL Server, Azure Data Lake e API externa (Pipedrive). Ideal para quem precisa otimizar o processo de análise financeira e atualização automática de dados em sistemas via API.

---

## 🧩 Funcionalidades principais

- 📅 Cálculo automático de intervalos de datas para análise histórica (últimos 365 dias).
- 🗄 Consulta e integração de dados provenientes de:
  - Banco de dados SQL Server
  - Azure Data Lake
- 🔄 Processamento e junção de dados para análises financeiras:
  - Pivot tables de faturamento total por grupo econômico
  - Classificação ABC automatizada para segmentação de clientes/grupos
- 📝 Geração de arquivos CSV para histórico e validação dos dados.
- 📊 Detecção de mudanças relevantes (status, curva ABC, datas de faturamento).
- 🔗 Atualização automática dos dados no sistema externo via API REST (Pipedrive).
- 📧 Envio de relatórios via email com status das atualizações e mudanças identificadas.
- 🗂 Organização automática de pastas para armazenar históricos e logs.

---

## ⚙️ Como funciona o código

1. **Definição do intervalo de datas**  
   Define o intervalo de análise para os últimos 365 dias a partir da data atual.

2. **Consulta de dados**  
   Obtém dados financeiros e cadastrais do SQL Server e Azure Data Lake.

3. **Processamento e análise**  
   - Explosão e limpeza de dados complexos (ex: múltiplos códigos de contratos).
   - Cálculo de faturamento total e acumulado por grupo econômico.
   - Classificação ABC baseada na curva de faturamento.
   - Análise temporal para identificar alterações em status e faturamento.

4. **Identificação de mudanças**  
   Detecta alterações em status, curva ABC, datas de primeiro e último faturamento, e status do grupo econômico.

5. **Atualização via API**  
   Envia atualizações dos dados para o sistema Pipedrive, registrando sucesso ou falha das requisições.

6. **Relatórios e histórico**  
   Salva arquivos CSV com dados históricos, relatórios de atualizações e envia email com os resultados.

---

## 📂 Estrutura de arquivos gerados

- `Historico/TB_TotalFaturamento.csv` — Faturamento agregado por grupo econômico  
- `Historico/TB_Final.csv` — Organizações com mudanças detectadas  
- `Historico/AtualizaçãoResumo.csv` — Status das atualizações via API  
- `Historico/TB_Organ_Final.csv` — Dados finais organizacionais completos

---

## 🛠️ Requisitos

- Python 3.x  
- Bibliotecas:
  - pandas
  - numpy
  - requests
  - beautifulsoup4
  - python-dotenv

- Variáveis de ambiente configuradas (ex: `API_TOKEN` para autenticação da API)

---

## 🚦 Como usar

1. Clone o repositório e instale as dependências:

   ```bash
   pip install -r requirements.txt
