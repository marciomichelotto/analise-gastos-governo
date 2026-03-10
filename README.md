# Análise dos Gastos do Governo por Ministério

Este projeto tem como objetivo analisar as despesas públicas dos ministérios da Educação, Saúde, Segurança e Defesa, com dados extraídos do Portal da Transparência.

## Objetivos
- Identificar padrões de gastos ao longo do tempo
- Comparar investimentos entre os ministérios
- Criar visualizações de fácil entendimento para tomada de decisão

## Ferramentas utilizadas
- Python (pandas, sqlalchemy, pyodbc)
- SQL Server
- Power BI
- Azure Data Factory (simulação de pipeline)

## Pipeline Python (CSV -> SQL Server)
Foi adicionado um pipeline em `pipeline_csv_to_sqlserver.py` para:
1. Ler arquivo CSV
2. Limpar e padronizar dados
3. Carregar dados no SQL Server

### Pré-requisitos
- Python 3.10+
- Pacotes: `pandas`, `sqlalchemy`, `pyodbc`
- Driver ODBC para SQL Server (ex.: `ODBC Driver 17 for SQL Server`)

### Exemplo de execução
```bash
python pipeline_csv_to_sqlserver.py \
  --csv-path data/GOVNOVO1.csv \
  --server localhost \
  --database Governo \
  --table dbo.Despesas \
  --trusted-connection \
  --if-exists append
```

## Resultados
Veja abaixo um exemplo do dashboard criado:

![Exemplo do Dashboard](link-da-imagem-aqui)

## Autor
Márcio Michelotto
