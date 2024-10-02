from google.cloud import datastore, storage  #google Cloud Datastore and Storage
import yfinance as yf                        #yahoo Finance
from datetime import datetime                #date and time
import pandas as pd                          
import matplotlib.pyplot as plt             

#Google Cloud project ID
PROJECT_ID = 'linear-listener-436516-c9'
#Google Cloud Storage bucket name
BUCKET_NAME = 'stock-bucket-4099'

#initialize datastore and storage clients
datastore_client = datastore.Client(project=PROJECT_ID)
storage_client = storage.Client()

def store_stock_data(ticker):
    #fetch stock data for the given ticker symbol
    stock = yf.Ticker(ticker)
    #latest data
    data = stock.history(period='1d')  

    #is data available?
    if data.empty:
        print("No data found for", ticker)
        return

    #new entity in Datastore with kind 'stockData'
    entity = datastore.Entity(datastore_client.key('stockData'))

    #set the properties of the entity with stock data
    entity.update({
        'ticker': ticker,                          #ticker symbol
        'price': round(data['Close'][0], 2),       #closing price, rounded to 2 decimal places
        'volume': int(data['Volume'][0]),          #trading volume
        'timestamp': datetime.utcnow()             #timestamp in UTC
    })

    #save the entity to Datastore
    datastore_client.put(entity)
    print(f"Stored data for {ticker}")

    #convert to Pandas (pd)
    df = pd.DataFrame(data)

    #plot and save the stock data
    plot_stock_data(df, ticker)

    #store the data in cloud Storage as a CSV file
    store_data_in_cloud(df, ticker)

def plot_stock_data(df, ticker): #matplotlib to plot data
    plt.figure(figsize=(10, 5))
    plt.plot(df['Close'], label='Closing Price', color = 'blue', linewidth=2)
    plt.title(f'{ticker} Stock Price')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    
    #save the plot(locally as .png file)
    plot_filename = f'{ticker}_stock_plot.png'
    plt.savefig(plot_filename)
    plt.show()

    #upload plot to cloud Storage
    upload_to_gcs(BUCKET_NAME, plot_filename, f'stocks/{plot_filename}')

def store_data_in_cloud(df, ticker):
    #select only the desired columns (price, volume, and date)
    selected_data = df[['Close', 'Volume']].copy()
    selected_data['Date'] = df.index  #date as a separate column

    #renmae columns
    selected_data.columns = ['Price', 'Volume', 'Date']

    #convert DataFrame to CSV and save locally
    csv_filename = f'{ticker}_stock_data.csv'
    selected_data.to_csv(csv_filename, index=False)

    #upload the CSV file to cloud Storage
    upload_to_gcs(BUCKET_NAME, csv_filename, f'stocks/{csv_filename}')

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    #upload to cloud Storage bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f'File {source_file_name} uploaded to {destination_blob_name}.')

def retrieve_stock_data(): #retrieve all stored data and display it
    
    #create a query for entities of kind 'stockData'
    query = datastore_client.query(kind='stockData')

    #fetch all entities matching the query
    results = query.fetch()

    #display each entity's data with iteration
    for entity in results:
        print(f"{entity['timestamp']} - {entity['ticker']}: ${entity['price']} (Volume: {entity['volume']})")

if __name__ == '__main__':
    #ticker
    ticker_symbol = 'MSFT'  # AMZN, MSFT
    store_stock_data(ticker_symbol)
    #retrieve and display all stored stock data
    retrieve_stock_data()
