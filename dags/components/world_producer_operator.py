from airflow.models import BaseOperator


class WorldProducerOperator(BaseOperator):
    def __init__(self, *, task_id: str = "say_world", name: str = "world", **kwargs):
        self.name = name
        self.output_name = self.output
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        return self.name
