from utils.base_component import BaseComponent
import requests
import os
from dotenv import load_dotenv
from typing import Dict


class BaseService(BaseComponent):
    def __init__(self,config,section_name):
        super().__init__(config,section_name)
        
    def initialize(self):
        load_dotenv()
        self.api_key = os.getenv('API_KEY')
        self.api_endpoint = self.config.get('api_endpoint')    
   
    def _request_response(self)-> Dict:
        try:
            headers = {'Authorization':f'Bearer{self.api_key}'}
            response = requests.get(self.api_endpoint,headers=headers)
            if response.status_code != 200:
                raise Exception(f'API response error:{response.status_code}')
                self.logger.exception(response.status_code,exc_info=True)
            else:
                return response.json()
        except Exception as e:
            self.logger.exception(e,exc_info=True)
            raise Exception(f'error:{e}')


    def run(self):
       self.initialize()
       response = self._request_response()