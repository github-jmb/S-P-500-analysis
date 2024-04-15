import os
import glob
import datetime as dt
from concurrent import futures

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import yfinance as yf


sp500_data_dir = "./data/sp500_stock_price"
nasdaq_data_dir = "./data/nasdaq100_stock_price"
os.makedirs(sp500_data_dir, exist_ok=True)
os.makedirs(nasdaq_data_dir, exist_ok=True)

tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
first_table = tables[0]
second_table = tables[1]

print(first_table.shape)
first_table["Symbol"] = first_table["Symbol"].map(lambda x: x.replace(".", "-"))  # rename symbol to escape symbol error 

first_table.to_csv("./data/SP500_20230409.csv", index=False)
first_table = pd.read_csv("./data/SP500_20230409.csv")

sp500_tickers = list(first_table["Symbol"])
first_table.head()

nasdaq_components = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
print(nasdaq_components.shape)

components_file_path = "./data/nasdaq100_20230409.csv"
nasdaq_components.to_csv(components_file_path, index=False)
nasdaq_components = pd.read_csv(components_file_path)
nasdaq100_tickers = list(nasdaq_components["Ticker"])

nasdaq_components.head()

def download_stock_data(start_time, end_time, output_dir, ticker_list):
    def download_stock(stock):
        try:
            print(stock)
            stock_df = yf.Ticker(stock).history(start=start_time, end=end_time)
            stock_df['Name'] = stock
            stock_df.index = stock_df.index.map(lambda x: x.strftime("%Y-%m-%d"))

            output_name = os.path.join(output_dir, f"{stock}.csv")
            stock_df.to_csv(output_name)
        except:
            bad_names.append(stock)
            print('bad: %s' % (stock))

    bad_names =[] #to keep track of failed queries

    #set the maximum thread number
    max_workers = 20

    now = dt.datetime.now()

    workers = min(max_workers, len(ticker_list)) #in case a smaller number of stocks than threads was passed in
    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(download_stock, ticker_list)

    """ Save failed queries to a text file to retry """
    if len(bad_names) > 0:
        with open(os.path.join(output_dir, "failed_queries.txt"),'w') as outfile:
            for name in bad_names:
                outfile.write(name+'\n')

    finish_time = dt.datetime.now()
    duration = finish_time - now
    minutes, seconds = divmod(duration.seconds, 60)
    print(f'The threaded script took {minutes} minutes and {seconds} seconds to run.')
    print(f"{len(bad_names)} stocks failed: ", bad_names)

  
          
start_time = dt.datetime(2021, 12, 1)
end_time = dt.datetime(2023, 4, 7)
download_stock_data(start_time, end_time, sp500_data_dir, sp500_tickers)
download_stock_data(start_time, end_time, nasdaq_data_dir, nasdaq100_tickers)

historical_stock_data_files = glob.glob(f"./{sp500_data_dir}/*.csv")

reference_day = "2022-12-30"
end_day = "2023-04-06"
price_change_list = []
tickers_to_ignore = []

for files in historical_stock_data_files:
    df = pd.read_csv(files, index_col=["Date"], parse_dates=True)
    ticker = os.path.splitext(os.path.basename(files))[0]
    try:
        price_close = df.loc[reference_day: end_day, ["Close"]]
        price_change = (price_close / price_close.loc[reference_day, "Close"] - 1) * 100
        price_change = price_change.iloc[1: ,:]
        price_change = price_change.rename(columns={"Close": ticker})
        price_change_list.append(price_change)
    except KeyError as e:
        # some stocks started trading after 2021-12-31
        print(ticker)
        tickers_to_ignore.append(ticker)
    
sp500_df = pd.concat(price_change_list, axis=1)
print(sp500_df.shape)

historical_stock_data_files = glob.glob(f"./{nasdaq_data_dir}/*.csv")

price_change_list = []

for files in historical_stock_data_files:
    nasdaq_df = pd.read_csv(files, index_col=["Date"], parse_dates=True)
    ticker = os.path.splitext(os.path.basename(files))[0]
    try:
        price_close = nasdaq_df.loc[reference_day: end_day, ["Close"]]
        price_change = (price_close / price_close.loc[reference_day, "Close"] - 1) * 100
        price_change = price_change.iloc[1: ,:]
        price_change = price_change.rename(columns={"Close": ticker})
        price_change_list.append(price_change)
    except KeyError as e:
        # some stocks started trading after 2021-12-31
        print(ticker)
    
nasdaq_df = pd.concat(price_change_list, axis=1)
print(nasdaq_df.shape)
nasdaq_df.head()

plt.figure(figsize=(10, 6))
plt.plot(sp500_df)
plt.title("SP500")
plt.xticks(rotation=90)
plt.xlabel("Date")
plt.ylabel("Performance(%)")
plt.ylim(-100, 100)
plt.show()
