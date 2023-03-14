
# ETL-Pipeline



## Install

- Get Docker for your OS [Here](https://docs.docker.com/get-docker/)

- Install the Docker Image for Apache Airflow following the steps:

```bash
  docker pull apache/airflow
```
- If you are in Mac/Linux run this command:
```bash
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
- Then use docker compose with the airflow init to create the airflow admin user

```bash
  docker-compose up airflow-init
```
- At last run the following to start the docker container

```bash
  docker-compose up
```
## Usage

- Start the Airflow running the following command in your terminal or command prompt:

```bash
    docker-compose up
```

- Open the Airflow UI in your web browser by navigating to http://localhost:8080
- LogIn with the airflow/airflow credentials
- Click on the "DAGs" link in the top menu bar to see a list of all available DAGs.
- Find the "csv_data_pipeline_daily" DAG and click the "Toggle DAG" button to enable it
- Once the DAG is enabled, you can click the "Trigger DAG" button to manually start the DAG. Alternatively, the DAG will run automatically according to the schedule defined in the schedule_interval parameter of the DAG object.
- Monitor the progress of the DAG in the Airflow UI. You can view the status of individual tasks, check the logs, and see the overall progress of the DAG.
## Autor

- [@Joao Victor Botelho](https://github.com/JVBotelho)

