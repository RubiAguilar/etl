from libs.mocks.environment_variables import *
from steps import Steps

print("Start ETL")
steps = Steps()

try:

    steps.registerStartInBDI()
    steps.readCSVFromS3DirectoryOut()
    steps.transformCSVToDataframe()
    steps.convertNaNToZeroInDataframeCSV()
    steps.generateDateframeForCDI()
    steps.assignDataTypeToEachColumnInDataframe()
    steps.readParquetsFromDatalake()
    steps.getEstimatedSalePriceBySkuFromDatalake()
    steps.mergeDataframesCDIAndDatalake()
    steps.mergeDataframesDatalakeAndPAPI()

except Exception as exception:
    print("An error ocurred in 'start'")
    print(f"Error: {exception}")
