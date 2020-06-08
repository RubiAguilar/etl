from ..utils.constants import *


class PAPIController:
    def __init__(self, repository):
        self.__repository = repository

    def getToken(self):
        try:
            method = "POST"
            url = f"{API_PAPI_PATH}/oauth/token"
            headers = {
                "content-type": "application/x-www-form-urlencoded",
                "authorization": f"Basic {API_PAPI_APIKEY}"
            }
            responseJSON = self.__repository.callAPI(
                method, url, headers, payload)
            self.__accessToken = responseJSON["access-token"]

            return True
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.papi': getToken")
            print(f"Error: {exception}")
            return False

    def getProductsBySkus(self, skus):
        try:
            method = "POST"
            query = self.__repository.getQueryFormatWithSkus(skus)
            body = {'query': query}
            payload = json.dumps(body)
            headers = {
                'Content-Type': 'application/json',
                'x-access-token': self.__accessToken
            }
            url = f"{API_PAPI_PATH}/products/codsap/key"
            responseJSON = self.__repository.callAPI(
                method, url, headers, payload)

            return responseJSON["data"]["list"]
        except Exception as exception:
            print("An error ocurred in 'libs.controllers.papi': getProductsBySkus")
            print(f"Error: {exception}")

            return {"data": "list": []}

    def getSkusByCountryAndCampaign(self, country_iso, catalogList, campaign_code):
        skuList = []
        method = "GET"
        for catalog in catalogList:
            url = '''
              {0}/products/campanapais/key?query={
                listCampanaPais(codpais:"{1}",codcatalogo="{2}",aniocampana="{3}"){
                  codsap
                }
              }
            '''.format(API_PAPI_PATH, country_iso, catalog, campaign_code)
            payload = {}
            headers = {
                'Content-Type': 'application/json',
                'x-access-token': self.__accessToken
            }
            responseJSON = self.__repository.callAPI(
                method, url, headers, payload)

            skuList += responseJSON["data"]["listCampanaPais"]

        return skuList
