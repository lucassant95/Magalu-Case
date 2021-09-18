#!/usr/bin/env bash


airflow db init
echo 'SQLite Initialized'
# start the web server, default port is 8080
airflow webserver -p 8080 &
# start the scheduler
airflow scheduler
