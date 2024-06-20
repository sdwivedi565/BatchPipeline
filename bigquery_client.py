from google.cloud import bigquery
import pandas as pd

class BigQueryClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def query_to_dataframe(self, sql):
        return self.client.query(sql).to_dataframe()

    def load_dataframe_to_table(self, dataframe, dataset_id, table_id, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
        job.result()
        print(f"DataFrame has been successfully written to BigQuery table '{self.project_id}.{dataset_id}.{table_id}'.")
