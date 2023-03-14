
# ETL-Pipeline



## Autor

- [@Joao Victor Botelho](https://github.com/JVBotelho)


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

- To finally connect to the airflow just run the docker as told in the install section and go to http://localhost:8080/ and SingIn with the credentials airflow/airflow

