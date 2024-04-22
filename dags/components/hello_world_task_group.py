from airflow.utils.task_group import TaskGroup
from components.hello_producer_operator import HelloProducerOperator
from components.world_producer_operator import WorldProducerOperator

HELLO_WORLD_TASK_GROUP_ID = "hello_world"


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
