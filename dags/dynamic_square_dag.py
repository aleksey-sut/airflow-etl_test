from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import timedelta

N = int(Variable.get("square_dag_n", default_var=10))

default_args = {
    'owner': 'etl_test',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def compute_square(i, **context):
    result = i * i
    context['ti'].xcom_push(key=f'square_{i}', value=result)
    print(f'{i} * {i} = {result}')

def aggregate_results(**context):
    ti = context['ti']
    results = []
    for i in range(1, N + 1):
        # Добавляем префикс группы
        square = ti.xcom_pull(
            key=f'square_{i}',
            task_ids=f'compute_squares.square_task_{i}'
        )
        results.append((i, square))

    print("🔢 Квадраты чисел:", results)

    # ✨ Отдаём список как return-значение,
    # тогда он автоматически сохранится в XCom под ключом 'return_value'
    return results

with DAG(
    dag_id='dynamic_square_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    description='DAG, который генерирует задачи квадратов чисел',
) as dag:

    with TaskGroup("compute_squares") as compute_group:
        for i in range(1, N + 1):
            PythonOperator(
                task_id=f'square_task_{i}',
                python_callable=compute_square,
                op_args=[i],
            )

    aggregator = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
    )

    compute_group >> aggregator
