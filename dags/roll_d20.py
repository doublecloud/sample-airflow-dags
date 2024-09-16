import os
import random
import pathlib
from datetime import datetime as dt

from airflow.decorators import dag, task


@dag(
    dag_id=pathlib.Path(__file__).stem,
    description="Example DAG with two chained tasks that outputs a random number between 1 and 20",
    schedule='*/5 * * * *',
    start_date=dt(2024, 9, 1, 0, 0, 0),
    catchup=False,
    tags=["sample", "random"],
)
def roll_d20():
    @task
    def dice_roll():
        seed = os.environ.get('RND_SEED_OVERRIDE')
        if seed:
            seed = int(seed)
            print("Random seed override:", seed)
            random.seed(seed)

        return random.randint(1, 20)

    @task
    def roll_result(roll_value):
        print("Hello from DoubleCloud")
        print("You rolled", roll_value)

    roll_result(roll_value=dice_roll())


my_dag = roll_d20()


if __name__ == '__main__':
    my_dag.test()
