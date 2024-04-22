from airflow.models import BaseOperator

HELLO_PRODUCER_OPERATOR_TASK_ID = "say_hello"


class HelloProducerOperator(BaseOperator):
    def __init__(
        self, *, task_id=HELLO_PRODUCER_OPERATOR_TASK_ID, hello: str = "hello", **kwargs
    ):
        self.hello = hello
        self.output_hello = self.output
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        return self.hello
