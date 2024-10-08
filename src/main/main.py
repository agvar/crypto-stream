from main.data_ingestion.base_producer import BaseProducer

def main():
    api_service = BaseProducer('config.ini','api')
    api_service.run()

if __name__=="__main__":
    main()