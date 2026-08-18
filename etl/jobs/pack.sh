#!/bin/bash
set -e
cd lambda/ingestion/layer
zip -r ../../../../../iac/artifacts/requests_layer.zip python
cd ..
zip -j ../../../../iac/artifacts/ingestion_lambda.zip main.py
cd ../plots_ingestion
zip -j ../../../../iac/artifacts/plots_ingestion_lambda.zip main.py
cd ../commercial_ingestion
zip -j ../../../../iac/artifacts/commercial_ingestion_lambda.zip main.py
cd ../commercial_rent_ingestion
zip -j ../../../../iac/artifacts/commercial_rent_ingestion_lambda.zip main.py