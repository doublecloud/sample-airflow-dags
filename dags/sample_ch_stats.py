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
    schedule=sample_table_dataset,
    start_date=datetime.datetime(2024, 9, 1, 0, 0, 0),
    catchup=False,
    template_searchpath=[sql_dir],
    dag_display_name="Compute stats on sample_table",
    tags=["sample", "clickhouse", "stats"],
    max_active_runs=1,
)
def sample_ch_insert():
    start = EmptyOperator(task_id="start")
    compute_stats = ClickHouseOperator(
        task_id='compute_stats_sample_table',
        sql="""
        SELECT 
            category,
            count() AS total_count,
            avg(value) AS mean_value,
            median(value) AS median_value,
            stddevPop(value) AS std_dev,
            min(value) AS min_value,
            max(value) AS max_value
        FROM sample_table
        GROUP BY category
        ORDER BY category;
        """,
        with_column_types=True,
        clickhouse_conn_id='ch_default',
    )

    @task
    def display_stats(upstream_xcom):
        stats, names = upstream_xcom
        print("Stats on sample_table:")
        print(",".join(t for t, _ in names))
        for row in stats:
            print(row)

    end = EmptyOperator(task_id="end")

    start >> compute_stats
    display_stats(compute_stats.output) >> end


my_dag = sample_ch_insert()


if __name__ == '__main__':
    my_dag.test()
