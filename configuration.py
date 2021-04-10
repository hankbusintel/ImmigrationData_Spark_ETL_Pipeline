import configparser
from sql_queries import *

class config():
    def get():
        """
        :return: configuration object defined in Immigration.cfg file.
        """
        config = configparser.ConfigParser()
        config.read_file(open(r'Immigration.cfg'))
        return config

MetaData = {
        "value i94cntyl":"Lui94cntyl",
        "value $i94prtl":"Lui94prtl",
        "value i94model":"Lui94mode",
        "value i94addrl":"Lui94addrl"
    }

dic_query = {
    "Lui94cntyl":Lui94cntyl_insert,
    "Lui94prtl":Lui94prtl_insert,
    "Lui94mode":Lui94mode_insert,
    "Lui94addrl":Lui94addrl_insert
    
}


dic_mainInsertList = {
    "Demographics":Demographics_insert,
    "airport":airport_insert,
    "tempreture":tempreture_insert,
    "immigration":immigration_insert
}


data_quality = [
        {'check_sql': "select count(*) from immigration where coalesce(cast(biryear as varchar),'') =''",'expected_result': 0},
        {'check_sql': "select count(*) from immigration where coalesce(cast(i94port as varchar),'') =''",'expected_result': 0}
]