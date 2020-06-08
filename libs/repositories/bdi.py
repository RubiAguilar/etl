import requests
import json
from ..utils.constants import API_URL_BDI


class BDIRepository:
    def callAPI(self, sessionCode, payload, path):
        try:
            endpoint = f"{API_URL_BDI}/{path}{sessionCode}"
            headers = {"content-type": "application/json"}
            response = requests.post(
                endpoint, data=json.dumps(payload), headers=headers)
            return True
        except Exception as exception:
            print("An error ocurred in 'libs.repositories.bdy': callAPI")
            print(f"Error: {exception}")
            return False
