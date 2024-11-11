from utils.base_component import BaseComponent
from utils.aws_connector import AWSConnector
import requests
import os
from dotenv import load_dotenv
from typing import Dict
import json
import time


class BaseProducer(BaseComponent):
    def __init__(self,config,section_name,aws_section):
        super().__init__(config,section_name)
        aws_section = self.read_config(config,aws_section)
        self.aws_connector = AWSConnector(self.logger,aws_section)
        
    def initialize(self):
        load_dotenv()
        self.api_key = os.getenv('API_KEY')
        self.api_endpoint = self.config.get('api_endpoint')    
        self.partition_key = self.config.get('partition_key')    
   
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
        
    def write_to_stream(self,dataset):
        records = [{'Data': json.dumps(record | {'Timestamp':dataset['timestamp']}).encode('utf-8'),
                    'PartitionKey': record['id']} for record in dataset['data']]
        self.aws_connector.write_to_kinesis_stream(records)

    def run(self):
        try:
            self.initialize()
            counter = 0
            while counter < 10:
                dataset = self._request_response()
                self.write_to_stream(dataset)
                time.sleep(30)
                counter += 1
            self.aws_connector.delete_streams()
            self.logger.info('API processing complete. Exiting script')
        except Exception as e:
            self.logger.error(e,exc_info=True)
      
    