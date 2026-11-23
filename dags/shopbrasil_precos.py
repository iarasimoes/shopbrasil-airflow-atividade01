"""Atividade 01 - Airflow: precos por categoria (ShopBrasil)."""

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_PRODUTOS = "https://fakestoreapi.com/products"
FUSO = "America/Sao_Paulo"


def avisar_falha(context):
    ti = context.get("task_instance")
    nome = ti.task_id if ti else "DAG"
    logging.error("[ALERTA SIMULADO] Falha em %s: %s", nome, context.get("exception"))


def avisar_retry(context):
    logging.warning("[RETRY] Nova tentativa: %s", context["task_instance"].task_id)


def avisar_sucesso(context):
    logging.info("[SUCESSO] %s", context["task_instance"].task_id)


@dag(
    dag_id="shopbrasil_precos_categoria",
    description="Resumo diario de precos por categoria da FakeStore API",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=FUSO),
    catchup=False,
    max_active_runs=1,
    tags=["atividade-01", "shopbrasil", "ecommerce"],
    on_failure_callback=avisar_falha,
)
def pipeline_shopbrasil():

    @task_group(group_id="ingestao")
    def ingestao():
        @task(
            retries=3,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            on_failure_callback=avisar_falha,
            on_retry_callback=avisar_retry,
            on_success_callback=avisar_sucesso,
        )
        def buscar_produtos():
            """Busca somente categoria e preco: XCom pequeno."""
            try:
                resposta = requests.get(API_PRODUTOS, timeout=30)
                resposta.raise_for_status()
                produtos = resposta.json()
                if not isinstance(produtos, list) or not produtos:
                    raise ValueError("A API nao retornou uma lista de produtos valida")

                return [
                    {"categoria": str(produto["category"]), "preco": float(produto["price"])}
                    for produto in produtos
                ]
            except (requests.RequestException, ValueError, KeyError, TypeError):
                logging.exception("Falha ao buscar produtos da FakeStore API")
                raise  # indispensavel para o Airflow acionar o retry

        @task
        def separar_categorias(produtos):
            """Descobre todas as categorias automaticamente, sem hardcode."""
            categorias = {}
            for produto in produtos:
                categorias.setdefault(produto["categoria"], []).append(produto["preco"])
            return [
                {"categoria": nome, "precos": precos}
                for nome, precos in sorted(categorias.items())
            ]

        return separar_categorias(buscar_produtos())

    @task_group(group_id="analise")
    def analise(categorias):
        @task(pool="ecommerce_pool")
        def calcular_metricas(item):
            """Uma instancia mapeada por categoria (fan-out)."""
            precos = item["precos"]
            return {
                "categoria": item["categoria"],
                "quantidade_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
            }

        @task
        def consolidar_metricas(resultados):
            """Reune os resultados das categorias (fan-in)."""
            return resultados

        por_categoria = calcular_metricas.expand(item=categorias)
        return consolidar_metricas(por_categoria)

    @task(on_failure_callback=avisar_falha)
    def salvar_postgresql(metricas):
        """UPSERT: data + categoria e a chave; nao duplica ao reprocessar."""
        contexto = get_current_context()
        data_ref = pendulum.instance(contexto["data_interval_end"]).in_timezone(FUSO).to_date_string()

        sql = """
            INSERT INTO resumo_precos_categoria
                (data_referencia, categoria, quantidade_produtos,
                 preco_medio, preco_minimo, preco_maximo)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (data_referencia, categoria)
            DO UPDATE SET
                quantidade_produtos = EXCLUDED.quantidade_produtos,
                preco_medio = EXCLUDED.preco_medio,
                preco_minimo = EXCLUDED.preco_minimo,
                preco_maximo = EXCLUDED.preco_maximo,
                atualizado_em = NOW()
        """

        hook = PostgresHook(postgres_conn_id="shopbrasil_db")
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                for linha in metricas:
                    cursor.execute(sql, (
                        data_ref,
                        linha["categoria"],
                        linha["quantidade_produtos"],
                        linha["preco_medio"],
                        linha["preco_minimo"],
                        linha["preco_maximo"],
                    ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logging.info("Persistidas %s categorias para %s", len(metricas), data_ref)
        return len(metricas)

    resumo = analise(ingestao())
    salvar_postgresql(resumo)


pipeline_shopbrasil()
