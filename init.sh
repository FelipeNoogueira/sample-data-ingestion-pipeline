#!/bin/sh
# Get containers running and healthy
docker compose down
docker compose up -d --wait
echo "Containers are running and healthy!"

# Define variables
WEB_SERVER_CONTAINER_ID=$(docker ps -f name=sample-data-ingestion-pipeline-airflow-webserver-1 -q)

# Delete config files from container
clean_files() {
    docker exec -u 0 $WEB_SERVER_CONTAINER_ID rm -rf /airflow_configs
}

# Import Airflow Connections and Variables from config files
import_configs() {
    docker cp $(pwd)/config/ $WEB_SERVER_CONTAINER_ID:/airflow_configs/
    docker exec $WEB_SERVER_CONTAINER_ID airflow connections import /airflow_configs/connections.json
    docker exec $WEB_SERVER_CONTAINER_ID airflow variables import /airflow_configs/variables.json
}

if import_configs; then
    echo "Configs were imported!"
else
    echo "Error while importing configs!"
fi

clean_files

