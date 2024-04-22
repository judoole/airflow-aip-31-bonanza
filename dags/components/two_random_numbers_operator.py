from airflow.models import BaseOperator
import random


class TwoRandomNumbersOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(TwoRandomNumbersOperator, self).__init__(*args, **kwargs)
        self.output_number_1 = self.output["number_1"]
        self.output_number_2 = self.output["number_2"]

    def execute(self, context):
        self.xcom_push(context, 'number_1', random.randint(1, 100))
        self.xcom_push(context, 'number_2', random.randint(1, 100))
