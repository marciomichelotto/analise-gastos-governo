"""Pipeline de ingestão CSV -> SQL Server.

Uso rápido:
python scripts/csv_to_sqlserver_pipeline.py \
  --csv-path data/entrada.csv \
  --server localhost \
  --database Governo \
  --table dbo.Despesas \
  --trusted-connection
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class PipelineConfig:
    csv_path: str
    table: str
    server: str
    database: str
    username: str | None
    password: str | None
    driver: str
    trusted_connection: bool
    if_exists: str
    chunksize: int


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(
        description="Lê um CSV, faz limpeza básica e carrega no SQL Server.",
    )
    parser.add_argument("--csv-path", required=True, help="Caminho do arquivo CSV")
    parser.add_argument("--table", required=True, help="Tabela destino (ex.: dbo.Despesas)")
    parser.add_argument("--server", required=True, help="Servidor SQL Server")
    parser.add_argument("--database", required=True, help="Nome do banco de dados")
    parser.add_argument("--username", help="Usuário SQL (opcional com trusted connection)")
    parser.add_argument("--password", help="Senha SQL (opcional com trusted connection)")
    parser.add_argument(
        "--driver",
        default="ODBC Driver 17 for SQL Server",
        help="Driver ODBC do SQL Server",
    )
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Usa autenticação integrada do Windows",
    )
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="append",
        help="Comportamento se a tabela já existir",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=5000,
        help="Tamanho do lote de carga no banco",
    )

    args = parser.parse_args()
    return PipelineConfig(
        csv_path=args.csv_path,
        table=args.table,
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
        driver=args.driver,
        trusted_connection=args.trusted_connection,
        if_exists=args.if_exists,
        chunksize=args.chunksize,
    )


def create_sqlserver_engine(cfg: PipelineConfig) -> Engine:
    if cfg.trusted_connection:
        conn_str = (
            f"mssql+pyodbc://@{cfg.server}/{cfg.database}"
            f"?driver={cfg.driver.replace(' ', '+')}&trusted_connection=yes"
        )
    else:
        if not cfg.username or not cfg.password:
            raise ValueError(
                "Informe --username e --password ou use --trusted-connection.",
            )
        conn_str = (
            f"mssql+pyodbc://{cfg.username}:{cfg.password}@{cfg.server}/{cfg.database}"
            f"?driver={cfg.driver.replace(' ', '+')}"
        )

    logging.info("Criando conexão com SQL Server em %s/%s", cfg.server, cfg.database)
    return create_engine(conn_str, fast_executemany=True)


def read_csv(path: str) -> pd.DataFrame:
    logging.info("Lendo CSV: %s", path)
    return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando limpeza dos dados")

    cleaned = df.copy()

    cleaned.columns = (
        cleaned.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    for col in cleaned.select_dtypes(include="object").columns:
        cleaned[col] = cleaned[col].astype("string").str.strip()
        cleaned[col] = cleaned[col].replace({"": pd.NA})

    date_hint_cols = [
        col
        for col in cleaned.columns
        if any(hint in col for hint in ["data", "dt_", "date"])
    ]
    for col in date_hint_cols:
        cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce")

    value_hint_cols = [
        col
        for col in cleaned.columns
        if any(hint in col for hint in ["valor", "preco", "quantidade", "qtd", "total"])
    ]
    for col in value_hint_cols:
        cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

    before = len(cleaned)
    cleaned = cleaned.drop_duplicates()
    after = len(cleaned)

    logging.info("Duplicados removidos: %s", before - after)
    logging.info("Total de linhas após limpeza: %s", after)

    return cleaned


def load_to_sql_server(df: pd.DataFrame, cfg: PipelineConfig) -> None:
    engine = create_sqlserver_engine(cfg)
    logging.info("Iniciando carga na tabela %s", cfg.table)

    df.to_sql(
        name=cfg.table.split(".")[-1],
        schema=cfg.table.split(".")[0] if "." in cfg.table else None,
        con=engine,
        if_exists=cfg.if_exists,
        index=False,
        chunksize=cfg.chunksize,
        method="multi",
    )

    logging.info("Carga finalizada com sucesso")


def main() -> None:
    setup_logging()
    cfg = parse_args()

    raw = read_csv(cfg.csv_path)
    cleaned = clean_data(raw)
    load_to_sql_server(cleaned, cfg)


if __name__ == "__main__":
    main()
