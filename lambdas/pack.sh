#!/bin/bash
cd ingestion/layer
zip -r ../../../iac/artifacts/requests_layer.zip python
cd ..
zip -j ../../iac/artifacts/ingestion_lambda.zip main.py
cd ../bronze
zip -j ../../iac/artifacts/bronze_lambda.zip main.py
