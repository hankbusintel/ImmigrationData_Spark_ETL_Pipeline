
import pandas as pd


class sasDesc():
    def __init__(self):
        from configuration import config
        config = config.get()
        self.metaFile=config.get("File","MetaData")

    def getTitle(self,string,d):
        if string.strip() in d:        
            return d[string.strip()]

    def cleanRowData(self,string):
        """
        Remove tab, CR/LF, ssssssempty spaces.
        :param string: row string from pandas dataframe
        :return: cleansed new string
        """
        return string.replace(";","").replace("'","").replace("\n","").replace("\t","").strip()


    def cleancolumn(self,row):
        """
        :param row: each row string from pandas dataframe
        :return: cleansed new row.
        """
        col1,col2 = self.cleanRowData(row[0]),self.cleanRowData(row[1])
        newrow = (col1,col2)
        return newrow

    def getMetaDataDict(dic_meta):
        """
        :param dic_meta: MetaData dictionary from configuration.py
        :return: dataframe from sas file.
        """
        print ("Reading metadata files")
        sas = sasDesc()
        title=None
        l,dic=[],{}
        with open(sas.metaFile,'r') as file:
            for row in file:
                if  title is not None and ";" not in row:
                    l.append(sas.cleancolumn(row.split("=")))
                if title is None and ";" not in row:
                    title = sas.getTitle(str(row),dic_meta)
                if title is not None and ";" in row:
                    try:
                        l.append(sas.cleancolumn(row.split("=")))
                    except:
                        print ("Warning: there are more than one dilimeter in the row or the row is empty")
                    df=pd.DataFrame(l)
                    df.columns=["col1","col2"]
                    dic[title]=df
                    l=[]
                    title = None
        return dic