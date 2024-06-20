import requests
import json

class APIRequestHandler:
    def __init__(self, api_key, url, headers):
        self.api_key = api_key
        self.url = url
        self.headers = headers

    def make_api_request(self, body, key):
        print(f"Sending API request for Route: {key}")
        response = requests.post(self.url, headers=self.headers, json=body)
        json_response = response.json()
        if 'routes' in json_response:
            return json_response['routes'][0]
        return []

    @staticmethod
    def duration_to_seconds(duration_str):
        return float(duration_str.strip("s"))
