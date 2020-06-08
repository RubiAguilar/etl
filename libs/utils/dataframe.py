import pandas


class DataFrameOperations:
    @staticmethod
    def transformCSVToDataframe(data, types):
           try:
                dataFrame = pandas.read_csv(
                    data, sep="\t", na_values="NULL", encoding="utf-8", dtype=types, low_memory=False)
                print("Information about this dataframe")
                print(dataFrame.info())
                return dataFrame
            except Exception as exception:
                print("An error ocurred in 'utils.dataframe.readCSV'")
                print(f"Error: {exception}")
                return None

    @staticmethod
    def convertNaNToZero(dataframe, columns):
        return dataframe[colums].fillna(value=0)

    @staticmethod
    def removeDuplicateByColumn(dataframe, columnName):
        return dataframe[columnName].drop_duplicates(keep='first')
