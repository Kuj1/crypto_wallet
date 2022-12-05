import asyncio
import csv
import multiprocessing
import time
import os
import concurrent.futures

import aiohttp
from fake_useragent import UserAgent
from bs4 import BeautifulSoup


TIMEOUT = aiohttp.ClientTimeout(total=300, connect=5)
LINK = 'https://bscscan.com/txs/'
UA = UserAgent(verify_ssl=False)

headers = {
    'user-agent': f'{UA.random}'
}

data_dir = os.path.join(os.getcwd(), 'data')

if not os.path.exists(data_dir):
    os.mkdir(data_dir)


async def get_pages(url):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), timeout=TIMEOUT,
                                     headers=headers) as session:
        async with session.get(url) as resp:
            body = await resp.text()
            soup = BeautifulSoup(body, 'html.parser')

            page_count = soup.find('div', class_='d-md-flex justify-content-between my-3').\
                find('ul', class_='pagination').find_all('li', class_='page-item')[2].\
                find('span', class_='page-link').find_all('strong')[1].text.strip()

            return page_count

loop = asyncio.new_event_loop()
count_pages = loop.run_until_complete(get_pages(url=LINK))


async def get_data(page, count):
    wall_set = set()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), timeout=TIMEOUT,
                                     headers=headers) as session:
        print(f'Page #{page}/{count}')
        page_url = f'https://bscscan.com/txs?p={page}'
        async with session.get(page_url) as resp:
            body = await resp.text()
            soup = BeautifulSoup(body, 'html.parser')

            table_wall = soup.find('div', attrs={'id': 'paywall_mask'}).find('table', class_='table').\
                find('tbody').find_all('tr')

            for tr in table_wall:
                id_wall = tr.find_all('td')[6]
                try:
                    hash_tag = id_wall.find('span', class_='hash-tag').find('a').text.strip()
                    wall_set.add(hash_tag)
                except AttributeError:
                    del AttributeError

    with open(os.path.join(data_dir, 'wall.csv'), 'a', encoding='utf-8') as doc:
        writer = csv.writer(doc)
        for wallet in wall_set:
            writer.writerow((wallet,))


def concat(page, count):
    asyncio.run(get_data(page, count))


def main():
    start = time.time()
    workers = multiprocessing.cpu_count()

    futures = []
    length_data = int(count_pages)

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for page in range(1, length_data + 1):
            new_future = executor.submit(
                concat,
                page=page,
                count=count_pages
            )
            futures.append(new_future)
            length_data -= 1

    concurrent.futures.wait(futures)
    stop = time.time()
    print(stop - start)


if __name__ == '__main__':
    main()
