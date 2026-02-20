from extract import connect_to_api, extract_json
from producer_setup import init_producer, topic
import time

def main():
    response = connect_to_api()
    
    data = extract_json(response)
    
    producer = init_producer()
    for stock in data:
        result = {
            'date': stock['timestamp'],  # timestamp from API (e.g. "2024-02-19 13:30:00")
            'symbol': stock['symbol'],
            'open' : stock['open'],
            'high' : stock['high'],
            'low' : stock['low'],
            'close' : stock['close'],
        }
        
        
        producer.send(topic, result)
        
        # print(f"Sent to Kafka: {result}")
        print(f'Data sent to Kafka topic "{topic}" successfully.')
        
        time.sleep(2)
        # print(result)
        
    producer.flush() # Ensure all messages are sent to Kafka before closing
    producer.close() # Close the producer connection
        
    return None


if __name__ == "__main__":
    main()