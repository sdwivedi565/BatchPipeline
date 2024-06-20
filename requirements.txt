# Route Optimization Project

This project aims to extract, transform, and load route optimization data using Google BigQuery,Fast API, and Airflow.

- `bigquery_client.py`: Handles interactions with Google BigQuery.
- `slack_notification.py`: Manages sending notifications to Slack.
- `api_request.py`: Handles API requests to external services.
- `main.py`: FastAPI application to expose endpoints.
- `route_dag.py`: Airflow DAG definition to orchestrate ETL process.
