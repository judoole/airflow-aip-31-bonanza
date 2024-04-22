from example import DAG_ID
from hamcrest import has_property, has_entry, equal_to
from conftest import TurbineBDD
from components.hello_world_task_group import HELLO_WORLD_TASK_GROUP_ID
from components.hello_producer_operator import HELLO_PRODUCER_OPERATOR_TASK_ID


def test_echo_hello_world_task(bdd: TurbineBDD):
    bdd.given_dag(DAG_ID)
    bdd.given_xcom(
        task_id=f"{HELLO_WORLD_TASK_GROUP_ID}.{HELLO_PRODUCER_OPERATOR_TASK_ID}",
        value="hei",
    )
    bdd.given_xcom(task_id="hello_world.say_world", value="verden")
    bdd.given_task("echo_hello_world")
    bdd.when_I_render_the_task_template_fields()
    bdd.then_it_should_match(has_property("env", has_entry("HELLO", equal_to("hei"))))
    bdd.then_it_should_match(has_property("env", has_entry("NAME", equal_to("verden"))))


def test_print_two_numbers_task(bdd: TurbineBDD):
    bdd.given_dag(DAG_ID)
    bdd.given_xcom(
        task_id="random_numbers",
        key="number_2",
        value=22,
    )    
    bdd.given_task("print_a_random_number")
    bdd.when_I_render_the_task_template_fields()
    bdd.and_I_execute_the_task()
    bdd.then_it_should_match(equal_to("The random number is: '22'"))
