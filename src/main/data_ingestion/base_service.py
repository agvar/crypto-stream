from utils.base_component import BaseComponent
import requests

class BaseService(BaseComponent):
    def __init__(self,config,section_name):
        super().__init__(config,section_name)
        
    def initialize(self):
        self.api_endpoint = self.config.get('api_endpoint')
        self.api_key = self.config.get('api_key')

    def _send_request(self):
        headers = {'Authorization':f'Bearer{self.api_key}'}

    def run(self):
        pass