
# ETL-Pipeline

This is a Python ETL pipeline designed to ingest data on a daily basis while ensuring data consistency, quality, and integrity before saving the treated data. The pipeline is responsible for:

- Retrieving data from a CSV dataset
- Normalizing and cleaning the data
- Dropping any duplicate rows
- Validating the processed data to ensure that it meets the expected criteria
- Saving the processed data to a new CSV dataset
The pipeline is designed to run on a daily basis to ensure that the processed dataset is up-to-date. The pipeline also includes a failure handling mechanism to help detect and resolve errors quickly.

Refer to the Roadmap section for more details about upcoming improvements and features.

## Solution Explain

This DAG retrieves data from a CSV dataset, normalizes and cleans it, and saves it to another CSV dataset daily. It consists of six tasks, each performing a specific data processing task. The DAG runs daily and is triggered by a schedule interval of timedelta(days=1).

The retrieve_data task reads the raw dataset from a specified file path and pushes the data to the next task. The validate_raw_data task checks if the raw dataset contains all expected columns, and raises a ValueError if any column is missing. The normalize_data task performs data normalization by converting all object-type columns to lowercase. The clean_data task drops any rows with null values. The drop_duplicates task removes any duplicate rows in the data. The save_data task writes the processed data to a specified file path.

The DAG also includes functions to handle errors and validate the processed dataset for expected columns, null values, and data types. If any errors occur, the on_failure_callback function logs the error message to the DAG audit log.

In addition, this DAG explicitly sets the data type of certain columns in the processed dataset, which helps ensure data consistency and avoids data type mismatches.

## Balancing Feature dev and tech debt

When working on an artifact that has been around for a while and is showing signs of decreasing performance, it's important to balance feature development with technical debt. This is especially important when working as part of a team, as different team members may have different priorities.

One way to balance feature development and technical debt is to prioritize the most critical issues that are impacting the performance of the artifact. This could involve identifying and addressing bottlenecks, improving data quality, or refactoring code to make it more efficient. It's important to involve the whole team in this process, as different team members may have different perspectives and insights into the performance issues.

Another approach is to prioritize technical debt over new features for a period of time in order to make the necessary improvements to the artifact. This may involve dedicating a sprint or two to addressing technical debt, rather than focusing on new feature development.

It's also important to continuously monitor and measure the performance of the artifact in order to identify any emerging issues and to ensure that the technical debt is not accumulating at an unsustainable rate.

Ultimately, balancing feature development and technical debt is a collaborative process that requires open communication, careful planning, and a willingness to prioritize performance and stability over new features when necessary.

## Tech Stack

- Apache Airflow
- Python
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
## Roadmap

- Fully change to the TaskFlow API paradigm
- Save to the PostgreSQL instead of a new .csv dataset
- Validate the raw data based in the PostgreSQL table columns
- Create empty dag schema to be used as model for new dags
- Improve error handling
- Improve DataType validation
- Improve performance

## Autor

- [@Joao Victor Botelho](https://github.com/JVBotelho)

