🚀 Automação de Análise e Atualização de Dados Corporativos
Bem-vindo ao projeto de automação que integra dados de múltiplas fontes, realiza análises financeiras e atualiza sistemas via API, tudo de forma automática! 💻✨

📋 Descrição Geral
Este script em Python faz a coleta, tratamento, análise e atualização de dados de faturamento e organizações, usando diversas fontes (banco SQL Server, Azure Data Lake) e integrações via API com o Pipedrive para manter informações atualizadas automaticamente.

⚙️ Principais Funcionalidades
1. 📅 Gerenciamento de Datas
Define intervalo dinâmico para análise (últimos 365 dias).

Ajusta datas para o formato ideal de consulta e análise.

2. 📊 Consulta e Tratamento de Dados
Extrai dados do banco SQL Server (consultandoSQL).

Busca dados complementares do Azure Data Lake (consultandoAzure).

Trata e explode listas de contratos para análise detalhada.

Realiza join entre bases para unir informações.

3. 💰 Análise de Faturamento
Cria tabela dinâmica (pivot table) para resumir faturamento por grupo econômico.

Ordena faturamento e calcula valores acumulados.

Classifica clientes em curva ABC para priorização (A, B, C) com códigos personalizados.

4. 🕒 Histórico Temporal e Verificação de Mudanças
Compara faturamento atual com histórico para detectar mudanças.

Cria colunas para identificar alterações em status, curva ABC e datas de faturamento.

Garante que dados inconsistentes ou ausentes sejam tratados.

5. 🔄 Atualização Automática via API Pipedrive
Atualiza informações na plataforma Pipedrive via requisições PUT.

Registra logs de sucesso, falhas e erros em requisições.

Controla envio para evitar sobrecarga na API.

6. 📧 Relatórios e Notificações
Gera arquivos CSV para auditoria e validação dos dados.

Envia e-mail com resumo da operação, status e grupos que tiveram mudanças.

🛠️ Tecnologias e Bibliotecas
Pandas & Numpy: manipulação e análise de dados.

Requests: comunicação HTTP com API Pipedrive.

BeautifulSoup: (importado, pode ser para extensões futuras de scraping).

Dotenv: gerenciamento seguro de variáveis de ambiente.

Logging: registro estruturado dos eventos e erros.

Funções customizadas: modularização com consultandoSQL, consultandoAzure, EnviarEmail, criar_dataframe_RDM e funções auxiliares.

📁 Estrutura de Pastas e Arquivos Gerados
Historico/ — pasta onde são salvos CSVs gerados para auditoria e históricos:

TB_TotalFaturamento.csv — resumo e classificação ABC.

TB_Final.csv — registros com alterações detectadas.

AtualizaçãoResumo.csv — status das atualizações na API.

TB_Organ_Final.csv — dados finais consolidados.

Outros arquivos temporários para análises intermediárias.

🚦 Como Usar
Configure seu arquivo .env com o token de API (API_TOKEN).

Garanta que as funções auxiliares (consultandoSQL, consultandoAzure, etc.) estejam implementadas e disponíveis.

Execute o script para processar dados, atualizar o Pipedrive e receber relatório por e-mail.

Verifique os arquivos na pasta Historico/ para auditoria.

🔍 Possíveis Melhorias Futuras
Adicionar tratamento e envio de erros mais robustos.

Implementar limitação dinâmica para requisições API para evitar throttling.

Utilizar multithreading/async para acelerar atualizações sem sobrecarregar a API.

Automatizar disparo de relatórios via outros canais (Slack, Teams).

Expandir uso do BeautifulSoup para coleta adicional de dados web.

🙌 Contribuições e Feedback
Contribuições são bem-vindas! Abra issues ou pull requests para melhorar funcionalidades, documentação e performance.

📧 Contato
Dúvidas ou sugestões? Me chame!
