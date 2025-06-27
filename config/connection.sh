#!/bin/bash

echo "✅ Registering Airflow connection: google_cloud_default"

airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra "{\"extra__google_cloud_platform__project\":\"sixth-topic-349709\",\"extra__google_cloud_platform__key_path\":\"/opt/airflow/config/gcp_service_account.json\"}"