# Projeto 1: Análise das Despesas do Governo Federal por Ministério

Este repositório reúne meu **primeiro projeto de Data Analytics** com dados públicos do Portal da Transparência.
A proposta é intencionalmente simples: organizar os dados e gerar uma análise clara da evolução dos gastos nos ministérios da **Educação, Saúde, Segurança e Defesa**.

## Objetivo do projeto
- Comparar despesas entre ministérios
- Observar evolução temporal dos gastos
- Apresentar os resultados em visualização de fácil leitura

## O que foi entregue
- Pipeline de ETL em Python (extração de CSV, tratamento e carga)
- Estrutura relacional no SQL Server para consulta analítica
- Dashboard no Power BI com comparativos e filtros por período/ministério

## Tecnologias utilizadas
- Python (`pandas`, `sqlalchemy`, `pyodbc`)
- SQL Server
- Power BI
- GitHub

## Pipeline Python (CSV -> SQL Server)
O script `pipeline_csv_to_sqlserver.py` executa:
1. Leitura do arquivo CSV
2. Limpeza e padronização dos dados
3. Carga no SQL Server

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

## Melhorias futuras (sem perder a simplicidade)
- Adicionar dicionário de dados com significado das colunas
- Incluir validações básicas de qualidade (nulos e duplicados)
- Publicar uma imagem real do dashboard neste README
- Criar rotina simples de atualização mensal dos dados

## Autor
Márcio Michelotto
