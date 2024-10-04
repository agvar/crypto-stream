import logging
from configparser import ConfigParser
from abc import ABC,abstractmethod
from dataclasses import dataclass    
from typing import Dict

class BaseComponent(ABC):
    logger = logging.getLogger(__name__)

    def __init__(self,config_file:str,section_name:str):
        self._setup_logging()
        self.config = self._read_config(config_file,section_name)

    def _setup_logging(self):
        self.logger.setLevel("INFO")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s- %(message)s",style='%')
        handler = logging.FileHandler("crypto_stream.log")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def _read_config(self,config_file:str,section_name:str) -> Dict :
        config = ConfigParser()
        config.read(config_file)
        try:
            return dict(config[section_name])
        except ConfigParser.NoSectionError as error:
            self.logger.error(error,exc_info=True) 

    @abstractmethod
    def initialize(self):
        pass
    
    @abstractmethod
    def run(self):
        pass


