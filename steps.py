from libs.controllers.bdi import BDIController
from libs.repositories.bdi import BDIRepository
from libs.controllers.papi import PAPIController
from libs.repositories.papi import PAPIRepository
from libs.utils.constants import *
from libs.utils.s3 import S3Operations
from libs.utils.dataframe import DataFrameOperations
from libs.utils.types import *
import pandas as pd
import numpy as np


class Steps:
    def __init__(self):
        repositoryBDI = BDIRepository()
        self.__controllerBDI = BDIController(repositoryBDI)
        repositoryPAPI = PAPIRepository()
        self.__controllerPAPI = PAPIController(repositoryPAPI)

    def registerStartInBDI(self):
        self.__controllerBDI.writeAction(SESSION_API_LOG, API_LOG_NAME_SERVICE)
        self.__controllerBDI.writeState(
            SESSION_API_LOG, "info", "Start process Linked Tactics")

    def readCSVFromS3DirectoryOut(self):
        self.__dataFileCSV = S3Operations.readFile(
            BUCKET_CSV_NAME, BUCKET_CSV_KEY)

    def transformCSVToDataframe(self):
        self.__dataFrameCSVDirectoryOut = DataFrameOperations.transformCSVToDataframe(
            self.__dataFileCSV, columnsCSVTypesGeneral)

    def convertNaNToZeroInDataframeCSV(self):
        self.__dataFrameCSVDirectoryOut[LIST_COLUMNS_CSV_USEFUL] = DataFrameOperations.convertNaNToZero(
            self.__dataFrameCSVDirectoryOut, LIST_COLUMNS_CSV_USEFUL)

    def generateDateframeForCDI(self):
        self.__dataframeCDICopyFromDataframeOriginal = self.__dataFrameCSVDirectoryOut
        self.__dataframeCDICopyFromDataframeOriginal[
            "FACTORCUADRE_CDI"] = self.__dataframeCDICopyFromDataframeOriginal["FACTORCUADRE"].str.split(".").str[0]
        self.__dataframeCDICopyFromDataframeOriginal[
            "CODPRODUCTO_DLK"] = self.__dataframeCDICopyFromDataframeOriginal["CODPRODUCTO"].astype(str)

    def assignDataTypeToEachColumnInDataframe(self):
        self.__dataFrameCSVDirectoryOut.astype(columnsCSVTypesSpecific)
        self.__dataframeCDICopyFromDataframeOriginal.astype(
            columnsCSVTypesSpecific)

    def readParquetsFromDatalake(self):
        country = self.__dataFrameCSVDirectoryOut.at[0, "CODPAIS"]
        campaign_year = str(
            self.__dataFrameCSVDirectoryOut.at[0, "ANIOCAMPANA"])
        bucketName = BUCKET_NAME_FUNCTIONAL_DLK
        key = S3_PATH_FUNCTIONAL_DLK + "/codpais=" + \
            country + "/aniocampana=" + campaign_year

        self.__dataframeFromDatalake = S3Operations.readMultipleParquetsAndReturnDataframe(
            BUCKET_NAME_FUNCTIONAL_DLK)

    def getEstimatedSalePriceBySkuFromDatalake(self):
        self.__dataframeFromDatalake["estvtamnneto"] = self.__dataframeFromDatalake["estvtamnneto"].astype(
            int)
        dataFrameGroupBySku = self.__dataframeFromDatalake.groupby(
            "codsap").apply(lambda x: x.nlargest(1, columns=['estvtamnneto']))
        self.__dataframeFromDatalake = dataFrameGroupBySku.reset_index(
            drop=True)

    def mergeDataframesCDIAndDatalake(self):
        self.__dataframeMergeCDIAndDatalake = pd.merge(
            self.__dataframeCDICopyFromDataframeOriginal, self.__dataframeFromDatalake, left_on='CODPRODUCTO_DLK', right_on='codsap', how='left')

        self.__dataframeMergeCDIAndDatalake.fillna(0)

    def mergeDataframesDatalakeAndPAPI(self):
        dataframeSkusWithoutDuplicate = DataFrameOperations.removeDuplicateByColumn(
            self.__dataframeMergeCDIAndDatalake, "CODPRODUCTO")
        skuList = dataframeSkusWithoutDuplicate.astype(str).to_list()

        self.__dataframeMergeCDIAndDatalake["CODPRODUCTO"] = self.__dataframeMergeCDIAndDatalake["CODPRODUCTO"].astype(
            str)

        productList = self.__controllerPAPI.getProductsBySkus(skuList)
        dataframeProductsFromPAPI = pd.DataFrame(productList).rename(
            columns={"codsap": "codsap1"})

        self.__dataframeMergeDatalakeAndPAPI = pd.merge(self.__dataframeMergeCDIAndDatalake, dataframeProductsFromPAPI, left_on=[
            'CODPRODUCTO'], right_on=['codsap1'], how='left')

        self.__dataframeMergeDatalakeAndPAPI.drop(
            ['codsap1'], axis=1, inplace=True)

        self.__dataframeMergeDatalakeAndPAPI["desnombreproductowebredes"] = self.__dataframeMergeDatalakeAndPAPI.replace(
            np.nan, None)

        if self.__dataframeMergeDatalakeAndPAPI['desnombreproductowebredes'] is None:
            self.__dataframeMergeDatalakeAndPAPI['desnombreproductowebredes'] = self.__dataframeMergeDatalakeAndPAPI['DESPRODUCTO']
        else:
            self.__dataframeMergeDatalakeAndPAPI['DESCOMERCIALOFERTA'] = self.__dataframeMergeDatalakeAndPAPI['desnombreproductowebredes']
