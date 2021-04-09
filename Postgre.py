import configparser
config = configparser.ConfigParser()
config.read('Immigration.cfg')
import psycopg2


class postgre():
    
    def __init__(self):
        self.host=config.get("PostgreSql","host")
        self.port=config.get("PostgreSql","port")
        self.dbname=config.get("PostgreSql","dbname")
        self.user = config.get("PostgreSql","user")
        self.password = config.get("PostgreSql","pw")
        self.driver = config.get("PostgreSql","driver")
        self.url =  config.get("PostgreSql","url").format(self.host,self.port,self.dbname)
    
    #Write data from spark to staging
    def sparkWriteToPostgre(self,dataframe,table):
        
        dataframe.write.options(
             url=self.url, 
             user=self.user,
             password=self.password,
             driver=self.driver
        ).mode("overwrite").jdbc(url=self.url,table=table)
        
    def get(self):
        conn = psycopg2.connect(f"""host={self.host} dbname={self.dbname} 
                                user={self.user} password={self.password} port={self.port}""")
        cur = conn.cursor()
        conn.set_session(autocommit=True)
        return cur,conn
    
    def executeBatch(cur,queryList):
        for query in queryList:
            cur.execute(query)
            
    #Load Meta data
    def getInsertRowList(self,table,row):
        if table == "Lui94cntyl" or table == "Lui94mode":
            return (int(row.col1),row.col2,row.col2)
        elif table =="Lui94prtl":
            column2=row.col2.split(",")[0]
            if "," in row.col2:
                column3=row.col2.split(",")[1]
            else:
                column3=""
            return (row.col1,column2,column3,column2,column3)
        elif table == "Lui94addrl":
            return (row.col1,row.col2,row.col2)
    
    def upsertMetaData(self,table,cur,dic,query):
        df = dic[table]
        for i,row in df.iterrows():
            cur.execute(query,self.getInsertRowList(table,row))
            
    def upsertAllMetaData(self,cur,dic_source,dic_query):
        for table in dic_source:
            self.upsertMetaData(table,cur,dic_source,dic_query[table])

    