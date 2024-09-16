import datetime
import pathlib

from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators import sql
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import (
    ClickHouseBaseDbApiOperator,
)


class ClickHouseBranchSQLOperator(
    sql.BranchSQLOperator,
    ClickHouseBaseDbApiOperator,
):
    """
    temporary workaround for Airflow < 2.9.4
    see https://github.com/bryzgaloff/airflow-clickhouse-plugin/issues/87
    """

    pass


@dag(
    dag_id=pathlib.Path(__file__).stem,
    schedule=None,
    start_date=datetime.datetime(2024, 9, 1, 0, 0, 0),
    catchup=False,
    dag_display_name="Create sample_table",
    tags=["sample", "clickhouse", "ddl"],
    max_active_runs=1,
)
def sample_ddl_stats():
    check_tbl_exists = ClickHouseBranchSQLOperator(
        task_id='check_if_sample_table_exists',
        sql='EXISTS sample_table',
        conn_id='ch_default',
        follow_task_ids_if_true='do_nothing',
        follow_task_ids_if_false='create_sample_table',
    )

    do_nothing = EmptyOperator(task_id="do_nothing")
    create_tbl = ClickHouseOperator(
        task_id='create_sample_table',
        sql="""
	    CREATE TABLE IF NOT EXISTS sample_table
	    (
            id UInt32,
            value Float64,
            category Enum8('A' = 1, 'B' = 2, 'C' = 3)
        ) ENGINE = MergeTree() ORDER BY id;
        """,
        clickhouse_conn_id='ch_default',
    )

    check_tbl_exists >> [create_tbl, do_nothing]


my_dag = sample_ddl_stats()


if __name__ == '__main__':
    my_dag.test()
