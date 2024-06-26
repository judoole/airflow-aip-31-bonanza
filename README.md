# airflow-aip-31-bonanza
A demo project to show how you can use Airflow AIP-31 for dependency injection, and testing Airflow using some BDD fixtures.

## How to use it

Run either `make test` to see the tests under `tests` folder in action.

Or, `make run` to fire up an Airflow, where you mess around with the example DAG


This repo tries to showcase the following:

## Adding dependencies using xcom

This is a feature added in [AIP-31](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148638736). Basically, it adds the ability to add upstream dependencies using the `.output` variable of tasks, if the `.output` reference is used in a **templated** field of another task. The `.output` is basically the return value of the `execute` method of an Airflow Operator. Or, if the operator has multiple outputs, using the `xcom_push` function, it can be reference as a dictionary, for example like so `.output["key_1"]`

So, why is this useful? Well;

### Loose coupling

Using this approach makes DAGs more [loosely coupled](https://en.wikipedia.org/wiki/Loose_coupling), by not using the `{{ xcom_pull(...) }}` strings. With this we can move use tasks in different DAGs, and choose the templated fields ourselves. For example, you might want to create a "backfill DAG", that operates on parts of output that is already created. This backfill DAG can now just hardcode the references, where in the production pipeline we are using `.output`.

### More readable/traceable

#### Airflow UI

First of all, the Airflow UI will represent "the truth" better. You can always use the `>>` and `<<` to infer dependencies, but many times it doesn't show the true dependency. For example, let's say we have this setup:

```python
extract >> load >> check_load >> log_metrics_about_load >> transform
```

represented like this

```mermaid
graph LR;
extract-->load;
load-->check_load;
check_load-->log_metrics_about_load;
log_metrics_about_load-->transform;
```

But, the real representation, showing the true dependencies, could be created like this:
```python
extract = ExtractOperator(...)
load = LoadOperator(source=extract.output, ...)
check_load = CheckLoadOperator(source=load.output, ...)
log_metrics_about_load = LogMetricsOperator(source=load.output, ...)
transform = TransformOperator(source=load.output, ...)

# Force some dependencies
check_load >> log_metrics_about_load
check_load >> transform
```

and should thus be represented like this
```mermaid
graph LR;
extract-->load;
load-->check_load;
check_load-->log_metrics_about_load;
load-->log_metrics_about_load;
load-->transform;
check_load-->transform;
```

Now, we can easily see that the output of load is used in transform. And this becomes very useful, the bigger your DAGs are.

If one then tries to utilise less `<<` and `>>` and more `.output`, these dependencies are created for free. 

#### Find code references

If you are fortunate, you might find yourself having a lot of code, and be able to reuse a lot of your code as well. One trick, to easily find who is using the output of the Operator, is to add variables that are references to the xcom output. For example the [TwoRandomNumbersOperator](https://github.com/judoole/airflow-aip-31-bonanza/blob/b605cba81313b029a0329745158ad98c714be7e5/dags/components/two_random_numbers_operator.py):
```python
class TwoRandomNumbersOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(TwoRandomNumbersOperator, self).__init__(*args, **kwargs)
        self.output_number_1 = self.output["number_1"]
        self.output_number_2 = self.output["number_2"]

    def execute(self, context):
        self.xcom_push(context, 'number_1', random.randint(1, 100))
        self.xcom_push(context, 'number_2', random.randint(1, 100))
```

Now I can reference `output_number_1` [like so](https://github.com/judoole/airflow-aip-31-bonanza/blob/b605cba81313b029a0329745158ad98c714be7e5/dags/example.py#L54) (example using a `@task` decorator):
```python
print_a_random_number(two_random_numbers.output_number_2)
```

This has now the added benefit that I can easily find all codereferences in most used IDEs, like who is actually using the output of this Operator. And I also get codecompletion on the variable names.

You can even use this on TaskGroups, by extending the TaskGroup class, as [in this HelloWorldTaskGroup](https://github.com/judoole/airflow-aip-31-bonanza/blob/2e601b666901c3e754581e90c889c56202c080b8/dags/components/hello_world_task_group.py):
```python
class HelloWorldTaskGroup(TaskGroup):
    def __init__(
        self, hello="hello", name="world", group_id=HELLO_WORLD_TASK_GROUP_ID, **kwargs
    ):
        super().__init__(group_id=group_id, **kwargs)
        with self:
            hello = HelloProducerOperator(hello=hello)
            name = WorldProducerOperator(name=name)
        self.output_hello = hello.output_hello
        self.output_name = name.output_name
```
Now I can reference the `self.output_hello`, which is basically just a re-reference to `hello.output_hello` [in a DAG like so](https://github.com/judoole/airflow-aip-31-bonanza/blob/2e601b666901c3e754581e90c889c56202c080b8/dags/example.py#L47)

```python
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
```

### Caveats

There are some

1. You cannot see/render, or use the xcom, until the previous task has run.
2. All references to `.output` must be done in templated fields.
3. You cannot use references to `.output` inside strings, even if they are in a templated field. For example this: `my_task(text=f"Print this '{other_task.output}'")` will work for execute, but it will not add a upstream dependency to `other_task`. A rewrite to `my_task(text=other_task.output")` will work.
4. Some Operators will try to use the templated field inside the `__init__` method and fail. For example [GCSToGCSOperator tries to figure out if a wildcard is used](https://github.com/apache/airflow/blob/0d9d8fa9fa1368092083a581006bfc96ce57da17/airflow/providers/google/cloud/transfers/gcs_to_gcs.py#L275), and fails since the object will be an instance of `XComArg`. For these cases, you probably would like to rewrite the Operator.
5. Some operators doesn't have an output. For these cases I recommend creating a wrapper Operator, and use the `self.output_` in `__init__` for added readability.
   
## Gherkin style tests

Setting up tests for Airflow is not easy. The entire setup method etc requires deep insight into what is going on under hood in Airflow. It often times means creating a DagBag object, populating this with your DAG under test, and running unit tests on this. Many times you need some xcom as well, and especially if you start using Airflow as in this example. That is why it is very useful to have a test harness that is both quick and easy to add more tests. In this repository, the test harnessing is done in [conftest.py](https://github.com/judoole/airflow-aip-31-bonanza/blob/ef5ec9556adcebcfd1efeb1cd9d3ccc97b9a4bd2/tests/conftest.py) and used for example l[ike in test_example.py](https://github.com/judoole/airflow-aip-31-bonanza/blob/ef5ec9556adcebcfd1efeb1cd9d3ccc97b9a4bd2/tests/test_example.py):

```python
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
```
