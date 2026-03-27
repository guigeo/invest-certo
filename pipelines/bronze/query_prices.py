"""Ferramenta de consulta da Bronze via DuckDB."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Optional

import duckdb
import pandas as pd


BRONZE_BASE_PATH = Path("data/bronze/prices")
BRONZE_VIEW_NAME = "bronze_prices"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consulta os parquets da Bronze com DuckDB."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Caminho para o arquivo .sql com a consulta.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita apenas a exibicao no terminal, sem alterar a query.",
    )
    return parser


def validate_bronze_path(base_path: Path) -> None:
    if not base_path.exists():
        raise FileNotFoundError(
            f"Diretorio Bronze nao encontrado: {base_path}"
        )

    parquet_files = list(base_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo parquet encontrado em: {base_path}"
        )


def read_sql_file(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo SQL nao encontrado: {file_path}"
        )

    sql = file_path.read_text(encoding="utf-8").strip()
    if not sql:
        raise ValueError(f"Arquivo SQL vazio: {file_path}")

    return sql


def register_bronze_view(
    connection: duckdb.DuckDBPyConnection,
    base_path: Path,
) -> None:
    parquet_glob = str(base_path / "**/*.parquet").replace("'", "''")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {BRONZE_VIEW_NAME} AS
        SELECT *
        FROM read_parquet('{parquet_glob}')
        """
    )


def execute_query(
    sql: str,
    *,
    base_path: Path = BRONZE_BASE_PATH,
) -> pd.DataFrame:
    validate_bronze_path(base_path)

    connection = duckdb.connect(database=":memory:")
    try:
        register_bronze_view(connection, base_path)
        return connection.execute(sql).fetchdf()
    except duckdb.Error as exc:
        raise ValueError(f"Erro ao executar SQL: {exc}") from exc
    finally:
        connection.close()


def render_result(df: pd.DataFrame, limit: Optional[int] = None) -> str:
    if df.empty:
        return "Consulta executada com sucesso, mas nao retornou linhas."

    rows_to_show = df.head(limit) if limit is not None else df
    output = rows_to_show.to_string(index=False)

    if limit is not None and len(df) > limit:
        output = (
            f"{output}\n\n"
            f"Mostrando {limit} de {len(df)} linha(s) retornadas."
        )

    return output


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.limit is not None and args.limit <= 0:
            raise ValueError("O valor de --limit deve ser maior que zero.")

        sql = read_sql_file(Path(args.file))
        result = execute_query(sql)
        print(render_result(result, limit=args.limit))
        return 0
    except (FileNotFoundError, ValueError) as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
