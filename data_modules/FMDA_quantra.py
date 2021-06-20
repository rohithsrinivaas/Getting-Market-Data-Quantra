# Import the SimFin library
import simfin as sf

# Import the names used for easy access to SimFin's data-columns
from simfin.names import *

# Import the Yahoo! Finance library
import yfinance as yf

# Import data manipulation library
import pandas as pd

# Import tweepy package
import tweepy

# Ignore warnings
import warnings
warnings.filterwarnings("ignore")

import random 

# API key to access tweepy
def get_twitter_tokens():
    # To get Twitter API keys, follow the steps given in the 'Get Twitter API Keys - Guide.pdf'
    # This PDF is available in the course  
    return {
                    'consumer_key':"dSoOHSSAB1SAICgeL9Y4dw4zF",
                    'consumer_secret':"T3VfZwsYkxWwqTgN2zGZCiKQanPGDa0imUasr74YmVuuKaICgy",
                    'access_token':"1106096356551094273-9eoEGFIPWk8DVQibHsbwaPcUYep0Nj",
                    'access_token_secret':"y7Ye4bhx1gfL0KWrXGo3QnVRmEYqiYEobitVKopBiWEjK",
    }     

# This function returns API object
def get_tweepy_api():
    # Get the Twitter tokens
    twitter_tokens = get_twitter_tokens()
    consumer_key = twitter_tokens['consumer_key']
    consumer_secret = twitter_tokens['consumer_secret']
    # Create authentication object
    auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    # Create API object
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    return api

# This function returns Fred API key
def get_fred_api():
    # Get Fred API
    return 'b285464e5fa72ce91e85e880bfe33e5f'
    
# This function returns the cryptocompare API key
def get_cryptocompare_api():
    # Get cryptocompare API key
    keys_list= ['04360ed9da4fe6c70214967a84d558474e91d9c2287eac05bfc8ce1fd78e78d4',
                'af33ca7e58e4455550337525c0de6d197b6578156498d45c4711427a135b2464',
                '0b4abcf1d46ffdd010bf12c3873e8dfeef9e11df1fe502e4b7d2f407bfbf19c8']
    return random.choice(keys_list)
    
# Define the utility to get the fundamental data for any asset ticker
def get_fundamental_data(asset_ticker):    
    # Define the asset ticker for yfinance
    asset_yf_ticker = yf.Ticker(asset_ticker)
    
    # SimFin data-directory
    sf.set_data_dir('simfin_data/')

    # SimFin use the free data API key
    sf.load_api_key('free')

    # Set the market as US
    market = 'us'

    # Fetch quarterly income statements for all the tickers in SimFin database
    income_data_simfin_all_stocks = sf.load_income(variant='quarterly', market=market)

    # Get the quarterly income statement for the ticker
    income_data_simfin = income_data_simfin_all_stocks.loc[asset_ticker,:]

    # Get the quarterly income statement from yfinance
    income_data_yfinance = asset_yf_ticker.quarterly_financials.T

    """
    The mapping dictionary stores the column names from yfinance as key and
    the corresponding column name from Simfin as the value
    """
    income_data_mapping_dict =\
        {
            "Total Revenue" : "Revenue",
            "Operating Income" : "Operating Income (Loss)",
            "Income Before Tax" : "Pretax Income (Loss)",
            "Net Income" : "Net Income"
        }

    # Rename the columns in the income_data_yfinance DataFrame
    income_data_yfinance = \
        income_data_yfinance.rename(columns=income_data_mapping_dict)

    # The final column list for the merged DataFrame
    income_data_column_heads = [i[1] for i in income_data_mapping_dict.items()]

    # Trim the income_data_simfin to the final column list
    income_data_simfin = income_data_simfin[income_data_column_heads]

    # Trim the income_data_yfinance to the final column list
    income_data_yfinance = income_data_yfinance[income_data_column_heads]

    # Sort the income_data_yfinance to match the order as in the SimFin DataFrame
    income_data_yfinance = income_data_yfinance.sort_index(ascending=True)

    # Join the two DataFrames
    income_data = income_data_simfin.append(income_data_yfinance)

    # Fetch quarterly balance sheets for all the tickers in SimFin database
    balance_sheet_simfin_all_stocks = sf.load_balance(variant='quarterly', market=market)

    # Get the quarterly balance sheets for the ticker
    balance_sheet_simfin = balance_sheet_simfin_all_stocks.loc[asset_ticker,:]

    # Get the quarterly balance sheets from yfinance
    balance_sheet_yfinance = asset_yf_ticker.quarterly_balance_sheet.T

    """
    The mapping dictionary stores the column names from yfinance as key and
    the corresponding column name from Simfin as the value
    """
    balance_sheet_mapping_dict =\
        {
            "Total Assets" : "Total Assets",
            "Total Liab" : "Total Liabilities",
            "Total Current Assets" : "Total Current Assets",
            "Total Current Liabilities" : "Total Current Liabilities",
            "Total Stockholder Equity" : "Total Equity",
            "Retained Earnings" : "Retained Earnings",
            "Long Term Debt" : "Long Term Debt"
        }

    # Rename the columns in the balance_sheet_yfinance DataFrame
    balance_sheet_yfinance = \
        balance_sheet_yfinance.rename(columns=balance_sheet_mapping_dict)

    # The final column list for the merged DataFrame
    balance_sheet_column_heads = [i[1] for i in balance_sheet_mapping_dict.items()]

    # Trim the balance_sheet_simfin to the final column list
    balance_sheet_simfin = balance_sheet_simfin[balance_sheet_column_heads]

    # Trim the balance_sheet_yfinance to the final column list
    balance_sheet_yfinance = balance_sheet_yfinance[balance_sheet_column_heads]

    # Sort the balance_sheet_yfinance to match the order as in the SimFin DataFrame
    balance_sheet_yfinance = balance_sheet_yfinance.sort_index(ascending=True)

    # Join the two DataFrames
    balance_sheet_data = balance_sheet_simfin.append(balance_sheet_yfinance)

    # Fetch quarterly cash flow statements for all the tickers in SimFin database
    cash_flow_simfin_all_stocks = sf.load_cashflow(variant='quarterly', market=market)

    # Get the quarterly cash flow statements for the ticker
    cash_flow_simfin = cash_flow_simfin_all_stocks.loc[asset_ticker,:]

    # Get the quarterly cash flow statements from yfinance
    cash_flow_yfinance = asset_yf_ticker.quarterly_cashflow.T

    """
    The mapping dictionary stores the column names from yfinance as key and
    the corresponding column name from Simfin as the value
    """
    cash_flow_mapping_dict =\
        {
            "Total Cash From Operating Activities" : "Net Cash from Operating Activities",
            "Total Cashflows From Investing Activities" : "Net Cash from Investing Activities",
            "Total Cash From Financing Activities" : "Net Cash from Financing Activities"
        }

    # Rename the columns in the cash_flow_yfinance DataFrame
    cash_flow_yfinance = \
        cash_flow_yfinance.rename(columns=cash_flow_mapping_dict)

    # The final column list for the merged DataFrame
    cash_flow_column_heads = [i[1] for i in cash_flow_mapping_dict.items()]

    # Trim the cash_flow_simfin to the final column list
    cash_flow_simfin = cash_flow_simfin[cash_flow_column_heads]

    # Trim the cash_flow_yfinance to the final column list
    cash_flow_yfinance = cash_flow_yfinance[cash_flow_column_heads]

    # Sort the balance_sheet_yfinance to match the order as in the SimFin DataFrame
    cash_flow_yfinance = cash_flow_yfinance.sort_index(ascending=True)

    # Join the two DataFrames
    cash_flow_data = cash_flow_simfin.append(cash_flow_yfinance)

    # Return the income statement, balance sheet and cashflow data
    return income_data, balance_sheet_data, cash_flow_data