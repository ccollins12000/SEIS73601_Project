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

class NOAADataRequest:
    """An object for request data from an NOAA station
    
    attributes:
            _RESULTS (obj): A List object containing all of the results retrieve
    """
    def __init__(self, api_key):
        """The constructor for a data request for a NOAA station

        args:
            station_id (str): The id of the NOAA station. Example GHCND:USC00210075. Stations can be searched for at https://www.ncdc.noaa.gov/data-access/land-based-station-data/find-station
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
        metadata = request_data.get('metadata', {})
        self._RECORD_COUNT = metadata.get('count', 0)
        self._RECORDS_PER_PAGE = metadata.get('limit', 0)
        self._CURRENT_PAGE = metadata.get('offset', 0)
        if self._RECORD_COUNT == 0:
            self._PAGES = 0
        else:
            self._PAGES = math.ceil(self._RECORD_COUNT / self._RECORDS_PER_PAGE)

        #parse results, appends the reuslt list to the objects existing result list.
        self._RESULTS.extend(request_data.get('results', []))

    def request_result_page(self, start_date, end_date, page=None, data_set_id=None, location_id=None, station_id=None):
        """Get a page of data from the NOAA api

        args:
            offset (int): Which page of data to retrieve
            start_date (str): The start date in format yyyy-mm-dd
            end_date (str): The end date in format yyyy-mm-dd
            datasetid (str): The data set to retrieve. This can be passed as a list. Example: ['TMIN', 'TMAX', 'TAVG']
            locationid (str): The id of the location to retrieve data about. (You must past at least locationid or stationid)
            stationid (str): The id of the station to retrieve data about. (You must past at least locationid or stationid)
        returns:
            The reponse text as parsed json
        """
        headers = {'token': self._API_KEY}
        
        #setup parameters for request
        parameters = {
            'includemetadata':'true',
            'units': 'standard',
            'limit': 1000,  # maximum is 1000 https://www.ncdc.noaa.gov/cdo-web/webservices/v2#data,
            'startdate': start_date,
            'enddate': end_date
        }
        
        if data_set_id:
            parameters.update({'datasetid':data_set_id})
            
        if page:
            parameters.update({'offset':page})
            
        if location_id:
            parameters.update({'locationid':location_id})
            
        if station_id:
            parameters.update({'stationid':station_id})
        
        #make request
        response = requests.get(url=self._url, headers=headers, params=parameters)
        
        if response.status_code == 200:
            self.parse_response(json.loads(response.text))
            return {"status": response.status_code}
        else:
            return {"status": response.status_code}

        

if __name__ == "__main__":
    token = input('Enter your NOAA token: ')
    NOAA = NOAADataRequest(token)
    NOAA.request_result_page(
        start_date='2020-01-01', 
        end_date='2020-01-31',
        location_id = 'FIPS:02',
        data_set_id='GHCND'
        )
    df = pd.DataFrame(NOAA._RESULTS)
    df.to_csv('test.csv')






