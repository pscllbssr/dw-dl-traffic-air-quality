#!/bin/bash

# load .ENV-variables
export $(xargs < .env)

# connect
ssh -i student_airflow_server.pem ubuntu@$SERVER_IP

