from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime

from bigquery_client import BigQueryClient
from api_request import APIRequestHandler

app = FastAPI()

PROJECT_ID = 'test-analytics-prod'
DATASET_ID = 'landing'
SOURCE_TABLE_ID = 'analytics_po_routes'
DESTINATION_TABLE_ID1 = 'test_stg_routes_pricing'
DESTINATION_TABLE_ID2 = 'test_stg_routes_pricing_zone'
API_KEY = "1234"

URL = 'https://routespreferred.googleapis.com/v1alpha:computeRoutes'
HEADER = {
    "X-Goog-FieldMask": "routes.optimizedIntermediateWaypointIndex,routes.distanceMeters,routes.duration,routes.legs,routes.description,routes.warnings,routes.travelAdvisory",
    "X-Goog-Api-Key": API_KEY
}

bigquery_client = BigQueryClient(PROJECT_ID)
api_request_handler = APIRequestHandler(API_KEY, URL, HEADER)

class RouteRequest(BaseModel):
    warehouse_lat: float
    warehouse_long: float
    num_packages: int
    value_list: list

@app.post("/compute-routes/")
async def compute_routes(request: RouteRequest):
    body = construct_body(request.warehouse_lat, request.warehouse_long, request.num_packages, request.value_list)
    routes = api_request_handler.make_api_request(body, "test_route")
    return routes

def construct_body(warehouse_lat, warehouse_long, num_packages, value_list):
    body = {
        'origin': {"location": {"latLng": {"latitude": warehouse_lat, "longitude": warehouse_long}}},
        'intermediates': [{"vehicleStopover": True, "via": False, "location": {"latLng": {"latitude": lat, "longitude": long}}} for lat, long in value_list],
        'destination': {"location": {"latLng": {"latitude": warehouse_lat, "longitude": warehouse_long}}},
        'travelMode': "DRIVE",
        'optimizeWaypointOrder': True,
        'routingPreference': "TRAFFIC_UNAWARE"
    }
    return body
