import os


class EnvironmentVariables:
    @staticmethod
    def dictionary():
        try:
            dictionaryEnvironmentVariables = {
                "API_LOG_NAME_SERVICE": os.environ["API_LOG_NAME_SERVICE"],
                "API_LOG_NAME_APP": os.environ["API_LOG_NAME_APP"],
                "API_LOG_PREFIX_COUNTRY": os.environ["API_LOG_PREFIX_COUNTRY"],
                "API_LOG_URL": os.environ["API_LOG_URL"],
                "COLLECTION_NAME": os.environ["COLLECTION_NAME"],
                "SESSION_API_LOG": os.environ["SESSION_API_LOG"],
                "BUCKET_NAME": os.environ["BUCKET_NAME"],
                "S3_PATH": os.environ["S3_PATH"],
                "BUCKET_NAME_FUNCTIONAL_DLK": os.environ["BUCKET_NAME_FUNCTIONAL_DLK"],
                "S3_PATH_FUNCTIONAL_DLK": os.environ["S3_PATH_FUNCTIONAL_DLK"],
                "API_PAPI_PATH": os.environ["API_PAPI_PATH"],
                "API_PAPI_APIKEY": os.environ["API_PAPI_APIKEY"]
            }
            print("********************************")
            print("List of environment variables")
            print("********************************")
            for var in dictionaryEnvironmentVariables.keys():
                print(
                    f"Name: {var}, Value: {dictionaryEnvironmentVariables[var]}")
            return dictionaryEnvironmentVariables
        except Exception as exception:
            print("An error ocurred 'libs.utils.environment_variables': dictionary")
            print(f"Error: {exception}")
            return {}
