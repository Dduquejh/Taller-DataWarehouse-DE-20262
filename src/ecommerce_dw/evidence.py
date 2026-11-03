"""Consultas de evidencia sobre Redshift para los escenarios del taller.

Uso (dentro del contenedor de Airflow, o con scripts/warehouse.sh):

    python -m ecommerce_dw.evidence state        # conteos, totales y huella de cada hecho
    python -m ecommerce_dw.evidence reconcile    # hechos vs. fuente validada; falla si no cuadran
    python -m ecommerce_dw.evidence batches      # lotes cargados y su estado
    python -m ecommerce_dw.evidence runs         # ejecuciones del pipeline
    python -m ecommerce_dw.evidence quarantine   # rechazos por motivo y lote
    python -m ecommerce_dw.evidence scd2 --customer-id 1 --product-id 1
    python -m ecommerce_dw.evidence query "select ..."
"""

import argparse
import hashlib
import sys
from contextlib import closing

from .connections import redshift_connection

FACTS = [
    ("fct_sales", "sale_sk", "subtotal", "int_mysql__order_items__valid", "subtotal"),
    ("fct_reviews", "review_sk", None, "int_mongodb__reviews__valid", None),
    ("fct_navigation_events", "navigation_event_sk", None, "int_mongodb__navigation_events__valid", None),
]


def _rows(cursor, sql, params=None):
    cursor.execute(sql, params)
    columns = [column[0] for column in cursor.description]
    return columns, cursor.fetchall()


def _print_table(columns, rows):
    widths = [max(len(str(value)) for value in [name, *[row[i] for row in rows]]) for i, name in enumerate(columns)]
    print("  ".join(str(name).ljust(width) for name, width in zip(columns, widths)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(str(value).ljust(width) for value, width in zip(row, widths)))


def state(cursor):
    """Una línea por hecho: filas, llaves distintas, total y huella del contenido."""
    lines = []
    for table, key, amount, source, source_amount in FACTS:
        amount_sql = f"coalesce(sum({amount}), 0)" if amount else "0"
        cursor.execute(f"select count(*), count(distinct {key}), {amount_sql} from marts.{table}")
        rows, keys, total = cursor.fetchone()
        cursor.execute(f"select * from marts.{table} order by {key}")
        digest = hashlib.sha256(repr(cursor.fetchall()).encode()).hexdigest()[:16]
        source_amount_sql = f"coalesce(sum({source_amount}), 0)" if source_amount else "0"
        cursor.execute(f"select count(*), {source_amount_sql} from intermediate.{source}")
        source_rows, source_total = cursor.fetchone()
        lines.append((table, rows, keys, total, digest, source_rows, source_total))
    return ["fact", "rows", "distinct_keys", "total", "digest", "valid_source_rows", "valid_source_total"], lines


def reconcile(cursor):
    columns, lines = state(cursor)
    _print_table(columns, lines)
    failures = [line[0] for line in lines if line[1] != line[5] or line[2] != line[1] or line[3] != line[6]]
    if failures:
        print(f"ERROR: no conciliado: {', '.join(failures)}", file=sys.stderr)
        return 1
    print("OK: cada hecho coincide con su fuente validada y no tiene llaves duplicadas")
    return 0


QUERIES = {
    "batches": """
        select batch_id, raw_table, mode, watermark_from, watermark_to, row_count, status,
               loaded_at, processed_at
        from control.batch_entities
        order by batch_id desc, raw_table
        limit 60
    """,
    "runs": """
        select batch_id, dag_run_id, mode, reprocess, status, failed_tasks, raw_rows,
               dbt_tests_passed, dbt_tests_failed, finished_at
        from control.pipeline_runs
        order by finished_at desc
        limit 20
    """,
    "quarantine": """
        select 'mysql.customers' as entity, rejection_reason, _batch_id, count(*) as rows
        from quarantine.quarantine_mysql__customers group by 1, 2, 3
        union all select 'mysql.products', rejection_reason, _batch_id, count(*)
        from quarantine.quarantine_mysql__products group by 1, 2, 3
        union all select 'mysql.orders', rejection_reason, _batch_id, count(*)
        from quarantine.quarantine_mysql__orders group by 1, 2, 3
        union all select 'mysql.order_items', rejection_reason, _batch_id, count(*)
        from quarantine.quarantine_mysql__order_items group by 1, 2, 3
        union all select 'documentdb.reviews', rejection_reason, _batch_id, count(*)
        from quarantine.quarantine_mongodb__reviews group by 1, 2, 3
        union all select 'documentdb.navigation_events', rejection_reason, _batch_id, count(*)
        from quarantine.quarantine_mongodb__navigation_events group by 1, 2, 3
        order by 1, 2, 3
    """,
}


def scd2(cursor, customer_id, product_id):
    print(f"dim_customer, customer_id={customer_id}")
    _print_table(*_rows(cursor, """
        select customer_sk, city, department, valid_from, valid_to, is_current
        from marts.dim_customer where customer_id = %s order by valid_from
    """, (customer_id,)))
    print(f"\ndim_product, product_id={product_id}")
    _print_table(*_rows(cursor, """
        select product_sk, category, valid_from, valid_to, is_current
        from marts.dim_product where product_id = %s order by valid_from
    """, (product_id,)))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["state", "reconcile", "scd2", "query", *QUERIES])
    parser.add_argument("sql", nargs="?")
    parser.add_argument("--customer-id", type=int, default=1)
    parser.add_argument("--product-id", type=int, default=1)
    args = parser.parse_args()

    with closing(redshift_connection()) as connection, connection.cursor() as cursor:
        if args.command == "state":
            columns, lines = state(cursor)
            _print_table(columns, lines)
        elif args.command == "reconcile":
            return reconcile(cursor)
        elif args.command == "scd2":
            scd2(cursor, args.customer_id, args.product_id)
        elif args.command == "query":
            if not args.sql:
                parser.error("query necesita el SQL como argumento")
            _print_table(*_rows(cursor, args.sql))
        else:
            _print_table(*_rows(cursor, QUERIES[args.command]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
