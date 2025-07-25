#!/bin/bash

set -e

teardown() {
    set +e

    docker kill fusioncore; docker rm fusioncore
    docker kill discernpsql; docker rm discernpsql
    docker kill discerninflux; docker rm discerninflux
    docker kill sorcerer; docker rm sorcerer


    sudo rm -rf ./tmp

    set -e
}

show_help() {
    echo "Usage: ./util/setup-databases.sh [flags] <data location>"
    echo ""
    echo "This command sets up the influx and fusion core databases on your local machine with the volumes at <data location>"
    echo "This means you can easily stand up the databases for post hoc analysis"
    echo ""
    echo "-h/--help shows this menu"
    echo ""
    echo "location optionally allows you to specify the folder you'd like the data to be stored in."
    echo "   defaults to ./release/data"
    echo ""
}

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    shift
fi

location="$1"
if [ "$location" = "" ]; then
    echo "please specify the data location"    
    exit 1
fi
shift

if [ ! "$@" = "" ]; then
    echo "more than 1 argument supplied. should you use quotes?"
    exit 1
fi



echo "pulling remote discernpsql image"
docker pull registry.gitlab.com/mergetb/tech/instrumentation/discernpsql:latest
docker tag registry.gitlab.com/mergetb/tech/instrumentation/discernpsql   discernpsql



echo "starting discern influx container"
docker run -d \
    -p 8086:8086 \
    --name discerninflux \
    -e DOCKER_INFLUXDB_INIT_MODE=setup \
    -e DOCKER_INFLUXDB_INIT_ORG=ISI \
    -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN='BIGElHSa291FOkrliGaBVc7ksnGgQ4vALbkfJzRuH02T2XB8qouH0H3IkYTJACE-XZ-QYV664CH5655LkbQDIQ' \
    -e DOCKER_INFLUXDB_INIT_BUCKET=empty \
    -e DOCKER_INFLUXDB_INIT_USERNAME=default-user \
    -e DOCKER_INFLUXDB_INIT_PASSWORD=something \
    influxdb:2.7.11

sleep 5

echo "restoring database into discerninflux..."
docker cp $location/influx-data.tar.gz discerninflux:/backup.tar.gz
docker exec discerninflux sh -c "tar -xzf /backup.tar.gz -C / && influx restore --org ISI --token 'BIGElHSa291FOkrliGaBVc7ksnGgQ4vALbkfJzRuH02T2XB8qouH0H3IkYTJACE-XZ-QYV664CH5655LkbQDIQ' /backup"


echo "setting up discernpsql container..."
docker run -d \
    -p 5432:5432 \
    --name discernpsql \
    -e POSTGRES_PASSWORD=*Something* \
    -e POSTGRES_USER="postgres" \
    -e POSTGRES_DB=Discern \
    postgres:17.2-bullseye

# Wait for it to come online
sleep 10


echo "loading data into psql container..."
docker exec -i discernpsql psql -U postgres -d Discern < $location/postgres-data.sql
