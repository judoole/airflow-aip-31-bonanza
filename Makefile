AIRFLOW_VERSION=2.8.3
# Get the current Python 3 version
PYTHON_VERSION=$(shell python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)
# Create Airflow constraints url based on the Airflow version and Python version
AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  venv          to create a virtual environment using venv"
	@echo "  pip-install   to install the dependencies using pip"
	@echo "  run           to run a local airflow (includes venv and pip-install)"
	@echo "  help          to show this message"
	@echo "Python version: $(PYTHON_VERSION), Airflow version: $(AIRFLOW_VERSION)"	

# Target for creating a virtual environment using venv
venv:
	python3 -m venv .venv

pip-install: venv
	. .venv/bin/activate && pip install --upgrade pip apache-airflow==$(AIRFLOW_VERSION) pytest PyHamcrest --constraint $(AIRFLOW_CONSTRAINTS_URL)

test: pip-install
	export AIRFLOW__CORE__DAGS_FOLDER=$$(pwd)/dags; \
	. .venv/bin/activate && pytest --continue-on-collection-errors -v -rA --color=yes

# Runs a local airflow
run: pip-install
	# Create some default environment variables, and start the webserver and scheduler
	export AIRFLOW__CORE__LOAD_EXAMPLES=false; \
	export AIRFLOW__CORE__DAGS_FOLDER=$$(pwd)/dags; \
	export AIRFLOW_HOME=$$(pwd)/.airflow_home; \
	. .venv/bin/activate; \
	airflow db migrate; \
	airflow users create -u admin -p admin -f Peter -l Parker -r Admin -e spiderman@superhero.org; \
	airflow scheduler & airflow webserver --port 9080

# Kills all the airflow running processes
stop:
	ps aux | grep "master.*airflow-webserver\|airflow scheduler" | grep -v "grep" | awk '{print $$2}' | xargs kill