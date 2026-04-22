#!/bin/bash
set -e
cd lambda/ingestion/layer
zip -r ../../../../../iac/artifacts/requests_layer.zip python
cd ..
zip -j ../../../../iac/artifacts/ingestion_lambda.zip main.py
cd ../plots_ingestion
zip -j ../../../../iac/artifacts/plots_ingestion_lambda.zip main.py