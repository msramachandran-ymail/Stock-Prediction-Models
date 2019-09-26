import urllib3, shutil
import time
from datetime import datetime, date, time, timedelta
from multiprocessing.dummy import Pool as ThreadPool


def get_stock_list():
    with open('sp500.lst') as fp:
        return fp.read().splitlines()


def download(stock, start, end):
    c = urllib3.PoolManager()
    url = 'http://quotes.wsj.com/%s/historical-prices/download?MOD_VIEW=page&num_rows=6299.041666666667&range_days=6299.041666666667&startDate=%s&endDate=%s' % (
        stock, start, end)
    filename = 'dataset/%s.csv' % stock
    print('Downloading %s' % stock)
    try:
        with c.request('GET', url, preload_content=False) as res, open(filename, 'w') as out_file:
            length = 16 * 1024
            while 1:
                buf = res.read(length)
                buf = buf.decode('utf-8').replace(" ", "")
                if not buf:
                    break
                out_file.write(buf)
    except:
        pass


def download_csv(start, end):
    pool = ThreadPool(20)
    arg = []
    for stock in get_stock_list():
        arg.append([stock, start, end])
    pool.starmap(download, arg)


def main():
    today = datetime.combine(date.today() + timedelta(days=1), time())
    five_years_back = today - timedelta(days=10 * 365)
    # start, end = int(today.timestamp()), int(five_years_back.timestamp())
    # download_csv(start, end)
    download_csv(five_years_back.strftime('%Y/%m/%d'), today.strftime('%Y/%m/%d'))


if __name__ == '__main__':
    main()
