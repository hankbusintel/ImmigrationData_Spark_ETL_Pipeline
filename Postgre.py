import psycopg2


class postgre():
    
    def __init__(self):
        from configuration import config
        config = config.get()
        self.host= config.get("PostgreSql","host")
        self.port= config.get("PostgreSql","port")
        self.dbname= config.get("PostgreSql","dbname")
        self.user = config.get("PostgreSql","user")
        self.password = config.get("PostgreSql","pw")
        self.driver = config.get("PostgreSql","driver")
        self.url =  config.get("PostgreSql","url").format(self.host,self.port,self.dbname)
    
    #Write data from spark to staging
    def sparkWriteToPostgre(self,dataframe,table):
        """
        Write data to staging postgre sql table.
        :param dataframe:  spark dataframe
        :param table: staging table name
        :return: void
        """
        dataframe.write.options(
             url=self.url, 
             user=self.user,
             password=self.password,
             driver=self.driver
        ).mode("overwrite").jdbc(url=self.url,table=table)
        
    def get(self):
        """
        Get cursor and connection.
        Create connection postgre.
        :return: cur, conn
        """
        conn = psycopg2.connect(f"""host={self.host} dbname={self.dbname} 
                                user={self.user} password={self.password} port={self.port}""")
        cur = conn.cursor()
        conn.set_session(autocommit=True)
        return cur,conn
    
    def executeBatch(self,cur,queryList):
        """
        Batch execute sql queries.
        :param cur: cursor
        :param queryList: dictionary of queries.
        :return:void
        """
        for query_title in queryList:
            print ("Inserting table {}".format(query_title))  
            #print (queryList[query_title])
            cur.execute(queryList[query_title])
            #print ("error during Insertion")
            print ("Success")
            
    #Load Meta data
    def getInsertRowList(self,table,row):
        """
        :param table:
        :param row:
        :return:
        """
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
        """
        Ppsert the single table from input pandas data frame.
        :param table: meta data table name
        :param cur: cursor
        :param dic: input meta dataframe
        :param query: sql query
        :return:void
        """
        df = dic[table]
        for i,row in df.iterrows():
            cur.execute(query,self.getInsertRowList(table,row))
            
    def upsertAllMetaData(self,cur,dic_source,dic_query):
        """
        Loop through table dictionary from configuration.py, call
        upsertMetaData to insert each of them.
        :param cur: cursor
        :param dic_source: input dataframe
        :param dic_query: input query dictionary
        :return:
        """
        print ("Upserting meta data to Postgre sql.")
        for table in dic_source:
            self.upsertMetaData(table,cur,dic_source,dic_query[table])
        print ("Success")

    #check data quality dynamically
    def check(self,cur,query,exp_result):
        """
        Check sql query.
        :param cur: curosr
        :param query: sql qury
        :param exp_result: expected sql output
        :return: Sql output and is pass or not.
        """
        cur.execute(query)
        result = cur.fetchall()[0][0]
        return result,result==exp_result

    def check_data_quality(self,cur,data_quality_list):
        """
        Check data quality by parsing user defined query and expected output.
        :param cur: Postgre cursor
        :param data_quality_list: data_quality from configuration.py
        :return:void
        """
        for query in data_quality_list:
            expect_result =query["expected_result"]
            sql = query["check_sql"]
            print ("Checking query: {},exp_result={}".format(sql,expect_result))
            result,ispass = self.check(cur,sql,expect_result)
            if not ispass:
                print ("Check Failed")
                raise ValueError(f"Check Faield. There are {result} 'null' in the table. \
                             Expected value is {expect_result}")
            else:
                print ("Check passed")