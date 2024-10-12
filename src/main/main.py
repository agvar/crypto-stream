from data_ingestion.base_producer import BaseProducer

def main():
    producer = BaseProducer('config.ini','api','aws')
    response = producer.run()
    print(response)

if __name__=="__main__":
    main()