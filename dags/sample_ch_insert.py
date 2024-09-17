import datetime
import pathlib

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


sql_dir = pathlib.Path(__file__).absolute().parent / "sql"
sample_table_dataset = Dataset('clickhouse://sample_table')


@dag(
    dag_id=pathlib.Path(__file__).stem,
    schedule=None,
    start_date=datetime.datetime(2024, 9, 1, 0, 0, 0),
    catchup=False,
    template_searchpath=[sql_dir],
    dag_display_name="Insert data to sample_table",
    tags=["sample", "clickhouse", "random"],
    max_active_runs=1,
    params={
        'num_rows': 1_000_000,
    },
)
def sample_ch_insert():
    start = EmptyOperator(task_id="start")
    insert_data = ClickHouseOperator(
        task_id='insert_into_sample_table',
        sql='insert_into_sample_table.sql',
        clickhouse_conn_id='ch_default',
        params={
            "num_rows": "{{ params.num_rows }}",
        },
        outlets=[sample_table_dataset],
    )

    start >> insert_data


my_dag = sample_ch_insert()


if __name__ == '__main__':
    my_dag.test()
