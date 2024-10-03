from google.cloud import datastore, storage  #Google Cloud Datastore and Storage
import yfinance as yf                        #Yahoo Finance
from datetime import datetime                #date and time
import pandas as pd                          
import matplotlib.pyplot as plt             

#Google Cloud project ID
PROJECT_ID = 'linear-listener-436516-c9'
#bucket name
BUCKET_NAME = 'stock-bucket-4099'

#initialize datastore and storage clients
datastore_client = datastore.Client(project=PROJECT_ID)
storage_client = storage.Client()

def store_stock_data(ticker):
    #fetch data for given ticker symbol
    stock = yf.Ticker(ticker)
    #latest data (period='1d')
    data = stock.history(period='1d')  

    #is data available?
    if data.empty:
        print("No data found for", ticker)
        return

    #new entity in datastore with a unique key (avoids overwriting)
    entity = datastore.Entity(datastore_client.key('stockData', f"{ticker}_{datetime.utcnow().isoformat()}"))

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

    #convert to Pandas DataFrame
    df = pd.DataFrame(data)

    #append new stock data to existing CSV file
    append_to_csv(df, ticker)

    #plot and save the stock data
    plot_stock_data(df, ticker)

def append_to_csv(df, ticker):
    #desired columns
    selected_data = df[['Close', 'Volume']].copy()
    selected_data['Date'] = df.index

    #rename column
    selected_data.columns = ['Price', 'Volume', 'Date']

    #csv filename
    csv_filename = f'{ticker}_stock_data.csv'

    #if csv already exists, append new data
    try:
        #fead the existing data from csv
        existing_data = pd.read_csv(csv_filename)
        #concatenate new data with existing data
        selected_data = pd.concat([existing_data, selected_data], ignore_index=True)
    except FileNotFoundError:
        #file doesn't exist, start fresh
        pass  

    #save the updated frame to csv (overwrites old file)
    selected_data.to_csv(csv_filename, index=False)

    #upload csv to cloud
    upload_to_gcs(BUCKET_NAME, csv_filename, f'stocks/{csv_filename}')

def plot_stock_data(df, ticker): 
    df.index = pd.to_datetime(df.index) #datetime
    #matplotlib to plot stock data
    plt.figure(figsize=(10, 5))  #size of the plot
    plt.plot(df['Close'], label='Closing Price', color='blue', linewidth=2)  #closing price
    plt.title(f'{ticker} Stock Price')  #title
    plt.xlabel('Date')  #X-axis label
    plt.ylabel('Price')  #Y-axis label
    plt.legend()  #show legend
    plt.grid(True)  #show grid lines
    plt.xlim(df.index.min(), df.index.max()) #x-axis stretching too far, limit to data avalible

     #save the plot
    plot_filename = f'{ticker}_stock_plot.png'
    plt.savefig(plot_filename)
    plt.show()  #display
    
    #upload to cloud storage
    upload_to_gcs(BUCKET_NAME, plot_filename, f'stocks/{plot_filename}')

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    #upload to bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)  #upload file to specified destination
    
    print(f'File {source_file_name} uploaded to {destination_blob_name}.')

def retrieve_stock_data():  #retrieve all stored data and display it
    
    #create a query for entities of kind 'stockData'
    query = datastore_client.query(kind='stockData')

    #fetch all entities matching the query
    results = query.fetch()

    #display each entity's data using iteration
    for entity in results:
        print(f"{entity['timestamp']} - {entity['ticker']}: ${entity['price']} (Volume: {entity['volume']})")

if __name__ == '__main__':

    ticker_symbol = 'MSFT' #ticker symbol, MSFT for this week
    
    store_stock_data(ticker_symbol)
    
    #retrieve and display all stored stock data
    retrieve_stock_data()
