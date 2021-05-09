import os.path
from tqdm import tqdm
import yfinance as yf
from multiprocessing import Pool

FINANCE_DIR = "data/finance/stock"

def download_ticker_finance(ticker):
    t = yf.Ticker(ticker)
    info_filepath = os.path.join(FINANCE_DIR, ticker + "_info.csv")
    t.info.to_csv(info_filepath)
    afinancials_filepath = os.path.join(FINANCE_DIR, ticker + "_afinancials.csv")
    t.financials.to_csv(afinancials_filepath)
    qfinancials_filepath = os.path.join(FINANCE_DIR, ticker + "_qfinancials.csv")
    t.quarterly_financials.to_csv(qfinancials_filepath)
    abalance_filepath = os.path.join(FINANCE_DIR, ticker + "_abalance.csv")
    t.balance_sheet.to_csv(abalance_filepath)
    qbalance_filepath = os.path.join(FINANCE_DIR, ticker + "_qbalance.csv")
    t.balance_sheet.to_csv(qbalance_filepath)
    acashflow_filepath = os.path.join(FINANCE_DIR, ticker + "_acashflow.csv")
    t.cashflow.to_csv(acashflow_filepath)
    qcashflow_filepath = os.path.join(FINANCE_DIR, ticker + "_qcashflow.csv")
    t.cashflow.to_csv(qcashflow_filepath)
    aearnings_filepath = os.path.join(FINANCE_DIR, ticker + "_aearnings.csv")
    t.earnings.to_csv(aearnings_filepath)
    qearnings_filepath = os.path.join(FINANCE_DIR, ticker + "_qearnings.csv")
    t.quarterly_earnings.to_csv(qearnings_filepath)

if __name__ == "__main__":
    tickers = []
    with open("tickers") as f:
        content = f.read()
        tickers = content.split("\n")
        tickers = list(filter(lambda x: x != '', tickers))

    pool = Pool(100)
    results = []
    for ticker in tickers:
        res = pool.apply_async(download_ticker_finance, (ticker, ))
        results.append(res)

    for res in tqdm(results):
        res.wait()
