from data_ingestion.base_service import BaseService

def main():
    api_service = BaseService('config.ini','api')
    api_service.run()

if __name__=="__main__":
    main()