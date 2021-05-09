import yfinance as yf

tickers = []
with open("tickers") as f:
    content = f.read()
    tickers = content.split("\n")
    tickers = list(filter(lambda x: x != '', tickers))


