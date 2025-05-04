#!/bin/bash
cd ingestion/layer
zip -r ../../../../../iac/artifacts/requests_layer.zip python
cd ..
zip -j ../../../../iac/artifacts/ingestion_lambda.zip main.py
