"""# Example simple Dependency Injection in Airflow.
This DAG shows how to use the AIP-31 feature of Airflow to inject dependencies into a DAG.
Basically it extensively uses the .output property of the tasks to add dependenies between tasks.

As well as this, it also shows how you can:

1. Add readable variables for on tasks for easier finding usages of code.
2. Use TaskGroup to group tasks together, and add variables which references task outputs
3. Unit test that the output keys are correct for the tasks.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

from components.hello_world_task_group import HelloWorldTaskGroup
from components.two_random_numbers_operator import TwoRandomNumbersOperator


@task
def print_a_random_number(the_number: str):
    # Return the number as a string with some prefix
    return f"The random number is: '{the_number}'"


DAG_ID = "example_aip_31"
with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    start_date=datetime(1976, 8, 13),
):
    # This is a TaskGroup that contains two tasks, hello and name
    # The outputs of those two tasks can be referred to through references on the TaskGroup
    # variables hello_world.output_hello and hello_world.output_name
    # The references themselves are xcoms, and once used in a template field in a task,
    # the task will depend on the task that produced the xcom.
    hello_world = HelloWorldTaskGroup(
        hello="hola",
    )

    # This task will echo the output of the two tasks in the TaskGroup
    # It will also add the two tasks as upstream dependencies
    echo_hello_world = BashOperator(
        task_id="echo_hello_world",
        bash_command='echo "${HELLO} ${NAME}"',
        env={"HELLO": hello_world.output_hello, "NAME": hello_world.output_name},
    )

    two_random_numbers = TwoRandomNumbersOperator(task_id="random_numbers")

    # This shows how to use multiple outputs, and how to 
    # add upstream dependencies at the same time.
    print_a_random_number(two_random_numbers.output_number_2)
    print_a_random_number(two_random_numbers.output_number_1)
