import requests
import json


class PAPIRepository:

    @staticmethod
    def callAPI(method, url, headers, payload):
        try:
            response = requests(method, url, headers=headers, data=payload)
            responseUTF8 = response.text.encode("utf8")
            responseJSON = json.loads(responseUTF8)
            return responseJSON
        except Exception as exception:
            print("An error ocurred in 'libs.repositories.papi': callAPI")
            print(f"Error: {exception}")
            return {}

    @staticmethod
    def getQueryFormatWithSkus(skus):
        quoted = ['"' + sku + '"' for sku in skus]
        formatted = ','.join(quoted)
        query = '''
          {
              list(codsap: [{{codsaps}}]) {
                  codsap,
                  desnombreproductowebredes,
                  fotobulkwebredes,
                  desnombrearticulocatalogo
              }
          }
        '''

        return query.format('{{codsaps}}', formatted_codsaps)
