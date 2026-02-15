import requests

from config import logger, headers, url

url = "https://alpha-vantage.p.rapidapi.com/query"

def connect_to_api():
    stocks = ['TSLA', 'MSFT', 'GOOGL']
    json_response = []
    for stock in range(0, len(stocks)):
        querystring = {"function":"TIME_SERIES_INTRADAY",
                    #    "symbol":stocks[stock],
                       "symbol":f"{stocks[stock]}",
                       "outputsize":"compact",
                       "interval":"5min",
                       "datatype":"json"}
        try:
            
            response = requests.get(url, headers=headers, params=querystring)

            response.raise_for_status()  # Check for HTTP errors and pass them to the except block

            data = response.json()
            
            logger.info(f"Successfully fetched stock for {stocks[stock]}")
            
            json_response.append(data)

            print(response.json())
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {stocks[stock]}: {e}")
            break
        
    return json_response
        
        
    
    



def extract_json(response):
    records = []
    
    for data in response:
        symbol = data['Meta Data']['2. Symbol']
        
        for data_str, metrics in data['Time Series (5min)'].items():
            record = {
                'symbol': symbol,
                'timestamp': data_str,
                'open': metrics['1. open'],
                'high': metrics['2. high'],
                'low': metrics['3. low'],
                'close': metrics['4. close'],
                'volume': metrics['5. volume']
            }
            records.append(record)
            
    return records

