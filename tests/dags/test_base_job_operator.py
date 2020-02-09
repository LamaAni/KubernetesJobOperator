from airflow import DAG
from src.kubernetes_job_operator import KubernetesBaseJobOperator

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "tester", "start_date": days_ago(2), "retries": 0}

dag = DAG(
    "test-base-job-operator",
    default_args=default_args,
    description="Test base job operator",
    schedule_interval=None,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
job_operator = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(
    task_id="sleep", depends_on_past=False, bash_command="sleep 5", retries=3, dag=dag,
)
dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    depends_on_past=False,
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t1 >> [t2, t3]
