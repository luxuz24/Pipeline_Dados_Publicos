from contextlib import closing
from pathlib import Path
from typing import Any

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Query
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_db: str = "pipeline_dados"

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[1] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
app = FastAPI(
    title="API de Dados Públicos - SGS/BCB",
    description="Consulta de indicadores macroeconômicos modelados via dbt.",
    version="1.0.0",
)


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def query_rows(query: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    try:
        with closing(get_clickhouse_client()) as client:
            result = client.query(query, parameters=parameters or {})
            return [dict(zip(result.column_names, row)) for row in result.result_rows]
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Não foi possível consultar o ClickHouse.",
        ) from exc


@app.get("/", tags=["Health"])
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health", tags=["Health"])
def database_health_check() -> dict[str, str]:
    query_rows("SELECT 1")
    return {"status": "ok", "database": "ok"}


@app.get("/indicadores", tags=["Indicadores"])
def list_indicators(
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict[str, Any]]:
    return query_rows(
        f"""
        SELECT codigo_serie, nome_serie, mes_referencia,
               quantidade_registros, valor_medio, valor_minimo,
               valor_maximo, ultimo_valor, ultimo_extraido_em
        FROM {settings.clickhouse_db}.mart_indicadores_mensal
        ORDER BY mes_referencia DESC, codigo_serie
        LIMIT {limit}
        """
    )


@app.get("/indicadores/{codigo_serie}", tags=["Indicadores"])
def search_indicator_by_code(codigo_serie: int) -> list[dict[str, Any]]:
    rows = query_rows(
        f"""
        SELECT codigo_serie, nome_serie, mes_referencia,
               quantidade_registros, valor_medio, valor_minimo,
               valor_maximo, ultimo_valor, ultimo_extraido_em
        FROM {settings.clickhouse_db}.mart_indicadores_mensal
        WHERE codigo_serie = {{codigo_serie:UInt32}}
        ORDER BY mes_referencia DESC
        """,
        {"codigo_serie": codigo_serie},
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum dado encontrado para a série {codigo_serie}",
        )
    return rows