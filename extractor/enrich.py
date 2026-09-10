import logging
import os
import sys
from datetime import datetime

import clickhouse_connect
from google import genai


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("LLM-Enrichment")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "pipeline_dados")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "none").lower()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def setup_enrichment_table(client):
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.indicadores_enriquecidos (
            codigo_serie UInt32,
            nome_serie String,
            mes_referencia Date,
            resumo String,
            provedor String,
            modelo String,
            enriquecido_em DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(enriquecido_em)
        ORDER BY (codigo_serie, mes_referencia)
        """
    )


def get_latest_indicators(client):
    result = client.query(
        f"""
        SELECT codigo_serie, nome_serie, mes_referencia,
               quantidade_registros, valor_medio, valor_minimo,
               valor_maximo, ultimo_valor
        FROM {CLICKHOUSE_DB}.mart_indicadores_mensal
        ORDER BY mes_referencia DESC, codigo_serie
        LIMIT 1 BY codigo_serie
        """
    )
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def generate_gemini_summary(indicator: dict) -> str:
    client = genai.Client()
    prompt = (
        "Analise o indicador macroeconômico abaixo em português brasileiro. "
        "Responda em até três frases, sem inventar causas ou dados ausentes.\n\n"
        f"Série: {indicator['codigo_serie']} - {indicator['nome_serie']}\n"
        f"Mês: {indicator['mes_referencia']}\n"
        f"Registros: {indicator['quantidade_registros']}\n"
        f"Média: {indicator['valor_medio']}\n"
        f"Mínimo: {indicator['valor_minimo']}\n"
        f"Máximo: {indicator['valor_maximo']}\n"
        f"Último valor: {indicator['ultimo_valor']}"
    )
    interaction = client.interactions.create(model=GEMINI_MODEL, input=prompt)
    for output in getattr(interaction, "outputs", []):
        text = getattr(output, "text", None)
        if text:
            return text.strip()
    raise RuntimeError("Gemini retornou uma interação sem texto.")


def run_enrichment():
    if LLM_PROVIDER in {"", "none", "disabled"}:
        logger.info("Enriquecimento LLM desabilitado por configuração.")
        return
    if LLM_PROVIDER != "gemini":
        raise RuntimeError(
            f"Provedor LLM '{LLM_PROVIDER}' ainda não possui implementação. "
            "Use LLM_PROVIDER=gemini ou desabilite o enriquecimento."
        )
    if not os.getenv("GEMINI_API_KEY"):
        raise RuntimeError("GEMINI_API_KEY não configurada para o provedor Gemini.")

    client = get_clickhouse_client()
    setup_enrichment_table(client)
    indicators = get_latest_indicators(client)
    if not indicators:
        raise RuntimeError("Nenhum indicador mensal disponível para enriquecimento.")

    rows = []
    for indicator in indicators:
        logger.info(
            "Gerando resumo para a série %s do mês %s",
            indicator["codigo_serie"],
            indicator["mes_referencia"],
        )
        rows.append(
            (
                indicator["codigo_serie"],
                indicator["nome_serie"],
                indicator["mes_referencia"],
                generate_gemini_summary(indicator),
                LLM_PROVIDER,
                GEMINI_MODEL,
                datetime.now(),
            )
        )

    client.insert(
        table="indicadores_enriquecidos",
        data=rows,
        column_names=[
            "codigo_serie",
            "nome_serie",
            "mes_referencia",
            "resumo",
            "provedor",
            "modelo",
            "enriquecido_em",
        ],
    )
    logger.info("Enriquecimento concluído para %d indicadores.", len(rows))


if __name__ == "__main__":
    run_enrichment()