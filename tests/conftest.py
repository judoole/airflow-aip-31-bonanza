import pytest
import json
import os
import logging
from contextlib import contextmanager
from typing import Dict
from dataclasses import dataclass, field
from typing import Any, List

from airflow import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.operator import Operator
from airflow.models.taskinstance import TaskInstance
import pendulum
from airflow.utils.dates import days_ago
from hamcrest import assert_that
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.models.xcom import XCom

# Initialize logging
logging.basicConfig(level=logging.INFO)


# Context manager for setting environment variables temporarily
@contextmanager
def set_temporary_env_var(key, value):
    original_value = os.environ.get(key)
    os.environ[key] = value
    yield
    if original_value is not None:
        os.environ[key] = original_value
    else:
        del os.environ[key]


# Helper function to import Airflow variables from a file
def _import_helper(filepath):
    from airflow.models import Variable
    try:
        with open(filepath, 'r') as varfile:
            var = json.load(varfile)
    except Exception as e:
        logging.error(f"Invalid variables file: {e}")
        return

    n = 0
    try:
        for k, v in var.items():
            if isinstance(v, dict):
                Variable.set(k, v, serialize_json=True)
            else:
                Variable.set(k, v)
            n += 1
    except Exception as e:
        logging.error(f"Failed to set variable: {e}")
    finally:
        logging.info(f"{n} of {len(var)} variables successfully updated.")


# Fixture for setting up DAG bag with warnings
@pytest.fixture(scope="session")
def dag_bag_with_warnings():
    # Set the AIRFLOW__WEBSERVER__WEB_SERVER_NAME to localhost if not set
    if 'AIRFLOW__WEBSERVER__WEB_SERVER_NAME' not in os.environ:
        os.environ['AIRFLOW__WEBSERVER__WEB_SERVER_NAME'] = 'localhost'
    
    from airflow.models import DagBag
    from airflow.utils import db

    # Try to initialize Airflow database, catch exception if it's already been initialized
    try:
        db.initdb()
    except Exception:
        logging.info("Database already initialized")    

    # Get DAG folder. (This does not work in VS Code for projects using "dags" folder, instead of "src")
    dags_folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', 'src')    

    # Import variables
    variables_filepath = f'{dags_folder}/../airflow-variables.local.json'
    if os.path.isfile(variables_filepath):
        _import_helper(variables_filepath)

    # Capture warnings
    import warnings
    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")
        dagbag = DagBag(dag_folder=dags_folder, include_examples=False)

    yield dagbag, captured_warnings



@dataclass
class _XCom:
    task_id: str
    value: Any
    dag_id: str = None
    key: str = XCOM_RETURN_KEY    

    

    
@dataclass
class TurbineBDD:
    """A class for managing BDD context in relation to Airflow.
    This class contains methods for given, when, then, and other BDD keywords.
    The given statements save the state of the system, 
    the when statements act upon the state and save the result, 
    and the then statements verify the state.
    """

    dag_bag: DagBag
    dag_bag_warnings: list
    dag: DAG = None
    task: Operator = None
    # It just saves the latest thing we retrieve
    # Typically in a when statement
    it: Any = None
    # DAG context
    execution_date: pendulum.DateTime = days_ago(1)
    dag_run_conf: Dict = None
    xcoms: List = field(default_factory=list)
    task_instance: TaskInstance = None
    context: Dict = None

    ##############################################################
    # Given statements
    # These are used to save the state of the system
    ##############################################################

    def given_dag(self, dag_id: str):
        """Given a DAG with the given dag_id exists, and save it as the current DAG."""
        self.dag = self.dag_bag.get_dag(dag_id)
        self.it = self.dag
        assert self.dag is not None, f"DAG with id {dag_id} does not exist in the DagBag: {self.dag_bag.dags.keys()}"

    def given_task(self, task_id: str):
        """Given the task on the current DAG, and save it as the current task."""
        assert self.dag is not None, "You need to have a DAG before you can get a task using given_dag()"
        self.task = self.dag.get_task(task_id)
        self.it = self.task
        assert self.task is not None, f"Task with id {task_id} does not exist among the {self.dag.dag_id}'s tasks: {self.dag.task_ids}"

    def given_execution_date(self, execution_date: Any):
        """Given the execution date."""
        if isinstance(execution_date, str):
            self.execution_date = pendulum.parse(execution_date)
        elif isinstance(execution_date, pendulum.DateTime):
            self.execution_date = execution_date
        else:
            raise ValueError(f"execution_date must be a string or a pendulum.DateTime, got {type(execution_date)}")

    def given_xcom(self, task_id: str, value: Any, dag_id: str=None, key: str=None):
        """Add a new XCom value to the self.xcoms list"""
        if not key:
            key = XCOM_RETURN_KEY
        if not dag_id:
            dag_id = self.dag.dag_id
        self.xcoms.append(_XCom(task_id=task_id, value=value, dag_id=dag_id, key=key))

    def given_dag_run_conf(self, conf: Dict):
        """Given the dag run conf. The DAG run conf is a dictionary, specific to a DAG run.
        Typically, in turbineflow, this is used for trigger-DAGs, that supply information about the file event
        that triggered the DAG run.
        """
        self.dag_run_conf = conf

    ##############################################################
    # When statements
    # These are used to act upon the state and save the result
    ##############################################################
    
    def when_I_get_all_the_tasks_ids(self):
        """Retrieve all the task ids for the given dag_id."""
        self.it = self.dag.task_ids

    @provide_session
    def when_I_render_the_task_template_fields(self, session=None):
        """Render the task. This is useful for testing the templated fields."""
        # Delete all previous DagRuns and Xcoms
        session.query(DagRun).delete()
        session.query(XCom).delete()
            
        # Create a DagRun
        dag_run = self.dag.create_dagrun(
            #dag_id=self.dag.dag_id,
            run_id='test_dag_run',
            execution_date=self.execution_date,
            start_date=self.execution_date,
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
            session=session,
            conf=self.dag_run_conf
        )
        # Create the XComs
        for xcom in self.xcoms:
            x_ti = dag_run.get_task_instance(xcom.task_id, session=session)
            assert x_ti is not None, f"TaskInstance with task_id {xcom.task_id} does not exist in the DagRun: {dag_run.task_instances}"
            x_ti.refresh_from_task(self.dag.get_task(x_ti.task_id))
            x_ti.xcom_push(key=xcom.key, value=xcom.value, session=session)
                
        ti: TaskInstance = dag_run.get_task_instance(self.task.task_id, session=session)
        assert ti is not None, f"TaskInstance with task_id {self.task.task_id} does not exist in the DagRun: {dag_run.get_task_instances(session=session)}"
        ti.refresh_from_task(self.dag.get_task(ti.task_id))        
        # Render the template fields
        # This sets the rendered variables on the self.task instance
        # so we can access them late, in the then statements
        ti.render_templates()
        self.task_instance = ti

    def and_I_execute_the_task(self, context=None):        
        self.when_I_execute_the_task(context)

    def when_I_execute_the_task(self, context=None):
        """Execute the task and save the results."""        
        self.it = self.task_instance.task.execute(self.task_instance.get_template_context())

    ##############################################################
    # Then statements
    # These are used to verify the state
    ##############################################################

    def then_it_should_match(self, matcher: str):
        """Verify that "it" matches something.
        This is a simple wrapper around hamcrest's assert_that.

        See https://pyhamcrest.readthedocs.io/en/release-1.8/library/
        for more information on how to use hamcrest matchers.   
        """
        assert_that(self.it, matcher)


@pytest.fixture(scope="function")
def bdd(dag_bag_with_warnings) -> TurbineBDD:
    """A fixture for creating a TurbineBDD object. 
    We reuse the DagBag, because of the fixture(scope=session), 
    in order to make the tests quicker."""
    dagbag, captured_warnings = dag_bag_with_warnings
    return TurbineBDD(dag_bag=dagbag, dag_bag_warnings=captured_warnings)
