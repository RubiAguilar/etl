import boto3
import io
import pandas as pd


class S3Operations:
    @staticmethod
    def readFile(bucket_name, key, isParquet=False):
        try:
            clientS3 = boto3.client("s3")
            resourceS3 = boto3.resource("s3")
            objectS3 = resourceS3.Object(bucket_name=bucket_name, key=key)
            if not isParquet:
                dataS3 = io.StringIO(
                    objectS3.get()["Body"].read().decode("ISO-8859-1"))
            else:
                dataS3 = io.BytesIO(
                    objectS3.get()["Body"].read())

            return dataS3
        except Exception as exception:
            print("An error ocurred in 'libs.utils.s3': read")
            print(f"Error: {exception}")
            return None

    @staticmethod
    def readParquetAndReturnDataFrame(bucket_name, key):
        dataS3 = S3Operations.readFile(bucket_name, key, True)
        return pd.read_parquet(dataS3)

    @staticmethod
    def readMultipleParquetsAndReturnDataframe(bucket_name, key):
        if not key.endswith("/"):
            key = key + "/"

        clientS3 = boto3.client('s3')
        resourceS3 = boto3.resource('s3')
        keysS3 = [item.key for item in resourceS3.Bucket(bucket_name).objects.filter(Prefix=key)
                  if item.key.endswith('.parquet')]

        if not keysS3:
            print('No parquet found in', bucket_name, key)

        dataFramesGeneratedFromParquets = [S3Operations.readParquetAndReturnDataFrame(bucket_name=bucket_name, key)
                                           for key in keysS3]
        return pd.concat(dataFramesGeneratedFromParquets, ignore_index=True)
