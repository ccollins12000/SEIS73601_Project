# -*- coding: utf-8 -*-
"""
Created on Sun Dec  5 15:10:20 2021

@author: charl
"""

import requests
import json
import datetime
import math
import time
import pandas as pd
from dateutil import parser
from dateutil.relativedelta import relativedelta

class NOAADataRequest:
    """An object for request data from an NOAA station
    
    attributes:
            _RESULTS (obj): A List object containing all of the results retrieve
    """
    def __init__(self, api_key):
        """The constructor for a data request for a NOAA station

        args:
            DEPRECIATED station_id (str): The id of the NOAA station. Example GHCND:USC00210075. Stations can be searched for at https://www.ncdc.noaa.gov/data-access/land-based-station-data/find-station
            api_key (str): Your NOAA api key. You can request a key here: https://www.ncdc.noaa.gov/cdo-web/token
        """
        self._url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data/'
        self._API_KEY = api_key
        self._REQUEST_MADE = False
        self._RECORD_COUNT = None
        self._RECORDS_PER_PAGE = None
        self._CURRENT_PAGE = None
        self._PAGES = None
        self._RESULTS = []
    
    def set_api(self,end_point):
        self._url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/{end_point}".format(end_point=end_point)
        
    def parse_response(self, request_data):
        """Parase the metadata and results from a station data request. Updates the page attributes of the DataRequest object.
        
            NOAA data json reponses contain metadata about the number of records available.
            
            Example:
                "metadata": {
                    resultset": {
                    offset": 1,
                    count": 51,
                    limit": 52
                }

        args:
            request_data (obj): A parsed json response
        """
        # parse metadata
        
        metadata = request_data.get('metadata', {}).get('resultset', {})
        self._RECORD_COUNT = metadata.get('count', 0)
        self._RECORDS_PER_PAGE = metadata.get('limit', 0)
        self._CURRENT_PAGE = metadata.get('offset', 0)
        if self._RECORD_COUNT == 0:
            self._PAGES = 0
        else:
            self._PAGES = math.ceil(self._RECORD_COUNT / self._RECORDS_PER_PAGE)

        #parse results, appends the reuslt list to the objects existing result list.
        self._RESULTS.extend(request_data.get('results', []))

    def request_result_page(self, default=None, start_date=None, end_date=None, page=None, data_set_id=None, data_type_id = None, location_id=None, station_id=None, location_category_id=None, data_category_id=None):
        """Get a page of data from the NOAA api

        args:
            offset (int): Which page of data to retrieve
            default (str): If utilzing an API endpoint that return single records with an initial unnamed parameter. Such as a station details lookup
            start_date (str): The start date in format yyyy-mm-dd
            end_date (str): The end date in format yyyy-mm-dd
            page (int): For requests that return results split across multiple pages, set the offse here
            data_set_id (str): The data set to retrieve. This can be passed as a list. Example: ['TMIN', 'TMAX', 'TAVG']
            data_type_id (str): Specify if there a specific data points that you want from a certain data set
            location_id (str): The id of the location to retrieve data about. (You must past at least locationid or stationid)
            station_id (str): The id of the station to retrieve data about. (You must past at least locationid or stationid)
            data_category_id (str): Specify if there is a general data category that you are querying for
            
            For more information about the specific parameters for each API endpoint go to: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
            The api endpoint can be set with the set_api method
        returns:
            The reponse text as parsed json
        """
        #set the api key
        headers = {'token': self._API_KEY}
        
        #setup parameters for request
        parameters = {
            #'includemetadata':'true',
            'units': 'standard',
            'limit': 1000  # maximum is 1000 https://www.ncdc.noaa.gov/cdo-web/webservices/v2#data,
        }
        
        url = self._url
        
        if default:
            url += default
            
        if data_type_id:
            parameters.update({'datatypeid': data_type_id})
            
        if start_date:
            parameters.update({'startdate':start_date})
            
        if end_date:
            parameters.update({'enddate':end_date})
            
        if data_set_id:
            parameters.update({'datasetid':data_set_id})
            
        if page:
            parameters.update({'offset':page})
            
        if location_id:
            parameters.update({'locationid':location_id})
            
        if station_id:
            parameters.update({'stationid':station_id})
            
        if location_category_id:
            parameters.update({'locationcategoryid':location_category_id})

        if data_category_id:
            parameters.update({'datacategoryid':data_category_id})
        
        #make request
        response = requests.get(url=url, headers=headers, params=parameters)
        
        if response.status_code == 200:
            data = json.loads(response.text)
            self.parse_response(data)
            if default:
                return data
            return {"status": response.status_code}
        else:
            return {"status": response.status_code}


def get_station_summary(api_key, sation_id, start_date, end_date, delta):
    """Get all the daily summaries for a station between two years

        args:
            api_key (str): Your NOAA api key. You can request a key here: https://www.ncdc.noaa.gov/cdo-web/token
            station_id (str): The id of the NOAA station. Example GHCND:USC00210075. Stations can be searched for at https://www.ncdc.noaa.gov/data-access/land-based-station-data/find-station
            start_year (int): The year to start at
            end_year (int): The last year to retrieve
            delta (obj): A timedelta or relativedelta that specifies how much time to pull on each iteration. Most granular timeframe is a timedelta with days = 1
        returns:
            A pandas dataframe with all the data
            """
    NOAA = NOAADataRequest(api_key)
    NOAA.set_api('data')
    
    while start_date <= end_date:
        current_page = 0
        pages = 1
        while current_page < pages:
            current_page += 1
            NOAA.request_result_page(
                    start_date= str(start_date), 
                    
                    # add delta and substract 1 day so that date range goes from start date to one day before next
                    # iterations start date (day is most granular time value for GHCND data (daily summaries))
                    end_date= str(start_date + delta - datetime.timedelta(days=1)),
                    station_id = sation_id,
                    data_set_id='GHCND',
                    page=current_page
                    )
            
            current_page = NOAA._CURRENT_PAGE
            pages = NOAA._PAGES
            
            #if no data exists to page through then break from loop
            if current_page is None or pages is None:
                break
            
            #API limits to 5 requests per second
            time.sleep(.2)
        
        start_date += delta
    
    #results from each page are appended to list in _RESULTS property
    return pd.DataFrame(NOAA._RESULTS)


                       
if __name__ == "__main__":
    import datetime
    import os
    
    token = input('Enter your NOAA token: ')
    NOAA = NOAADataRequest(token)
    
    
    station_list = pd.read_csv('stations.csv')

    

    for station_index, station_row in station_list.iterrows():
        
        station = station_row['Close_Station']
        
        print(station_index, " : ", station )
        
        
        #station = 'GHCND:USW00023234'
        
        station_file_path = station.replace(":", "_") +".csv"
    
        if station_file_path in os.listdir():
            continue
    
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 12, 1)
        delta = relativedelta(months=1)
        get_station_summary(token, station, start_date, end_date, delta).to_csv(station_file_path)
        
        #.to_csv(station_file_path)
        
        

    
    #get data types
    #NOAA = NOAADataRequest(token)
    #NOAA.set_api('datatypes')
    #NOAA.request_result_page(start_date='2020-01-01', end_date='2020-01-31', data_set_id='GHCND')
    #pd.DataFrame(NOAA._RESULTS).to_csv('data_types.csv')

    #10,000
    
    #get data categories
    #NOAA = NOAADataRequest(token)
    #NOAA.set_api('datacategories')
    #NOAA.request_result_page(start_date='2020-01-01', end_date='2020-01-31')
    
    #Get US state summaries
    #NOAA = NOAADataRequest(token)
    #NOAA.set_api('locations')
    #NOAA.request_result_page(location_category_id='ST', data_category_id='GHCND')
    
    #Get stations in state
    #NOAA = NOAADataRequest(token)
    #NOAA.set_api('stations')
    #NOAA.request_result_page(location_id='FIPS:27', data_set_id='GHCND', start_date='2020-12-31', end_date='2020-12-31')
        
    #get station info
    #NOAA = NOAADataRequest(token)
    #NOAA.set_api('stations')
    #NOAA.request_result_page(default='WBAN:94948')
    







