from kafka import KafkaConsumer
import json
import time


# --------------CONFIGURATION MATCHING THE PRODUCER ------------------


consumer = KafkaConsumer(
    'stock_analysis',  # Kafka topic name
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='earliest',  # Start consuming from the earliest message
    enable_auto_commit=True,       # Automatically commit offsets
    group_id='stock_analysis_group',  # Consumer group ID
    # group_id='my-consumer-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
)


print("Consumer is listening to Kafka topic 'stock_analysis'.... Waiting for message on topic 'Stock_analysis'....")

for message in consumer:
    stock_data = message.value  # Get the stock data from the message
    
    print(f"Received stock data: {stock_data}") # Print the received stock data to the console
    
    # Simulate processing time
    time.sleep(1)
    
    print("Finished processing the stock data.\n")
    
consumer.close()  # Close the consumer connection when done
print("Kafka Consumer connection closed.")