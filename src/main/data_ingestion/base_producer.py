from utils.base_component import BaseComponent
from utils.aws_connector import AWSConnector
import requests
import os
from dotenv import load_dotenv
from typing import Dict


class BaseProducer(BaseComponent):
    def __init__(self,config,section_name,aws_section):
        super().__init__(config,section_name)
        aws_section = self.read_config(config,aws_section)
        self.aws_connector = AWSConnector(self.logger,aws_section)
        
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

    def push_to_queue(self,data):
        self.aws_connector.write_to_kinesis_stream(data)

    def run(self):
       self.initialize()
       response = self._request_response()