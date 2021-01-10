##
from fredapi import Fred
from src.constants import FRED_API, QUANDL_API, DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT
import src.utils.pg as pg
import sys


import yfinance as yf
# tickers = yf.Tickers('msft baba vfh')
# msft = tickers.tickers.MSFT
# tickers = yf.Tickers('VOX, VCR, VDC, VDE, VFH')

msft = yf.Ticker("VFH")


##
# get stock info
info = msft.info
print(info)
# 'sector','shortName'
##
# get historical market data
hist = msft.history(period="max")
# Index(['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'], dtype='object')
##
# show actions (dividends, splits)
actions = msft.actions
# Index(['Dividends', 'Stock Splits'], dtype='object')
##

# show dividends
dividends = msft.dividends  # series
##

# show splits
splits = msft.splits  # series
##

# show financials
financials = msft.financials
quarterly_financials = msft.quarterly_financials

# show balance sheet
balance_sheet = msft.balance_sheet
quarterly_balance_sheet = msft.quarterly_balance_sheet

# show cashflow
cashflow = msft.cashflow
quarterly_cashflow = msft.quarterly_cashflow

# show earnings
earnings = msft.earnings
quarterly_earnings = msft.quarterly_earnings
##
# show major holders
major_holders = msft.major_holders

# show institutional holders
institutional_holders = msft.institutional_holders



# show sustainability
sustainability = msft.sustainability

# show analysts recommendations
recommendations = msft.recommendations

# show next event (earnings, etc)
calendar = msft.calendar

# show ISIN code - *experimental*
# ISIN = International Securities Identification Number
msft.isin

# show options expirations
options = msft.options
##
# get option chain for specific expiration
opt = msft.option_chain('YYYY-MM-DD')
# data available via: opt.calls, opt.puts

data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30")

print('completed')

