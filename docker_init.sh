echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
