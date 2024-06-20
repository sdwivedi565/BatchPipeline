from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from bigquery_client import BigQueryClient
from slack_notification import SlackNotification
from main import construct_body, api_request_handler

PROJECT_ID = 'test-analytics-prod'
DATASET_ID = 'landing'
SOURCE_TABLE_ID = 'analytics_po_routes'
DESTINATION_TABLE_ID1 = 'test_stg_routes_pricing'
DESTINATION_TABLE_ID2 = 'test_stg_routes_pricing_zone'

bigquery_client = BigQueryClient(PROJECT_ID)
slack_notification = SlackNotification('slack_conn')

default_args = {
    "catchup": False,
    "max_active_runs": 1,
    "owner": "airflow",
    "retries": 2,
    'on_failure_callback': slack_notification.send_alert,
    "start_date": datetime(2023, 9, 11),
    "use_legacy_sql": False,
    "write_disposition": "WRITE_TRUNCATE",
}

dag = DAG(
    dag_id="test_routes_api_data",
    default_args=default_args,
    schedule_interval='@daily'
)

def extract_api_endpoint():
    sql = f"""SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_ID}`
        WHERE destination_latitude IS NOT NULL
        AND destination_longitude IS NOT NULL
        AND warehouse_lat IS NOT NULL
        AND warehouse_long IS NOT NULL"""
    df = bigquery_client.query_to_dataframe(sql)

    zip_base_route_group = df.groupby(['zip_base_route', 'destination_metro_code']).apply(lambda x: list(zip(x['destination_latitude'], x['destination_longitude'], x['warehouse_lat'], x['warehouse_long']))).reset_index(name='values')
    zip_base_route_data = []

    for _, row in zip_base_route_group.iterrows():
        key = (row['zip_base_route'], row['destination_metro_code'])
        value_list = row['values']
        warehouse_lat = value_list[0][2]
        warehouse_long = value_list[0][3]
        num_packages = len(value_list)
        body = construct_body(warehouse_lat, warehouse_long, num_packages, value_list)
        routes = api_request_handler.make_api_request(body, key)
        if routes:
            static_durations = [leg["staticDuration"] for leg in routes["legs"]]
            entire_route_duration = sum(api_request_handler.duration_to_seconds(d) for d in static_durations)
            stem_time = api_request_handler.duration_to_seconds(routes["legs"][0]["staticDuration"])
            last_leg_time = api_request_handler.duration_to_seconds(routes["legs"][-1]["staticDuration"])
            just_legs_duration = entire_route_duration - stem_time - last_leg_time

            subzone_price = ((just_legs_duration / 3600) * 25)
            stem_price = (stem_time / 3600) * 25
            overall_price = subzone_price + stem_price

            distance_meters = float(routes['distanceMeters'])
            description = routes['description']

            zip_base_route_data.append({
                "Route": row['zip_base_route'],
                "destination_metro_code": row['destination_metro_code'],
                "Warehouse_Latitude": warehouse_lat,
                "Warehouse_Longitude": warehouse_long,
                "Entire_Route_Duration": entire_route_duration,
                "Stem_Time": stem_time,
                "Last_Leg_Time": last_leg_time,
                "Just_Legs_Duration": just_legs_duration,
                "subzone_price": round(subzone_price, 2),
                "stem_price": round(stem_price, 2),
                "Distance_Meters": distance_meters,
                "Description": description,
                "Num_Packages": num_packages,
                "latest_update": datetime.now()
            })

    endpoint_df = pd.DataFrame(zip_base_route_data)
    bigquery_client.load_dataframe_to_table(endpoint_df, DATASET_ID, DESTINATION_TABLE_ID1)

    endpoint_df['zone'] = endpoint_df['Route'].str.extract('(\d+)').astype(int)
    agg_df = endpoint_df.groupby(['zone', 'destination_metro_code']).agg({
        "Warehouse_Latitude": 'first',
        "Warehouse_Longitude": 'first',
        "Entire_Route_Duration": 'sum',
        "Stem_Time": 'sum',
        "Last_Leg_Time": 'sum',
        "Just_Legs_Duration": 'sum',
        "subzone_price": 'sum',
        "stem_price": 'sum',
        "Distance_Meters": 'sum',
        "Num_Packages": 'sum',
        "latest_update": 'first'
    }).reset_index()

    bigquery_client.load_dataframe_to_table(agg_df, DATASET_ID, DESTINATION_TABLE_ID2)

extract_api_endpoint_task = PythonOperator(
    task_id='extract_api_endpoint',
    python_callable=extract_api_endpoint,
    provide_context=True,
    dag=dag
)
