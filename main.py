import asyncio
import json
import time
import requests
import os
import sys
import threading
import logging
from bs4 import BeautifulSoup
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36'
}


def category_reader():
    """Reads the initial categories to be read and extracted from mercado libre website"""
    with open(os.path.join(sys.path[0], 'categories.txt'), "r") as file:
        categories = file.read().splitlines()
    return categories


def mercadolibre_webcrawler(categories):
    """Create the threads for each category to extract the webpage data  """
    output = open('output.txt', 'w', encoding="utf-8")
    threads = []
    logging.info("Data Crawling is starting")
    for link in categories:
        t = threading.Thread(target=data_extractor, args=(link, output))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


def data_extractor(link, output):
    """Execution of the workers to extract the data from categories and its pages"""
    output_lock = threading.Lock()
    temp = 1
    response = requests.get(link, headers=headers)
    with output_lock:
        print(response.text, file=output)
    while temp < 10:
        soup = BeautifulSoup(response.text, "html.parser")
        next_page = soup.find('li', 'andes-pagination__button andes-pagination__button--next')
        if next_page:
            next_link = next_page.a.get('href')
            next_response = requests.get(next_link, headers=headers)
            with output_lock:
                print(next_response.text, file=output)
            response = next_response
            temp += 1
        else:
            break


def webscrap_products(url):
    """Execution of the workers to extract the data from each product

    Returns
    -------
        Product Description
        Actual Price
        Last Price
        Image thumbnail url
        Brand

    """

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    last_price_pattern = "Precio anterior: \s*(\d+)"
    brand_pattern = "(?<=Marca).*"

    description = soup.select_one('h1', class_='ui-pdp-title')
    url_image = soup.find('figure', class_='ui-pdp-gallery__figure')
    all_prices = soup.find('div', class_='ui-pdp-price mt-16 ui-pdp-price--size-large')
    price = soup.find("meta", itemprop="price")
    brand_row = soup.find(re.compile("^tr"))

    previous_price = re.findall(last_price_pattern, all_prices.text)
    last_price = previous_price[0] if previous_price else ''

    if brand_row: # check if the page has description table
        brand_result = re.findall(brand_pattern, brand_row.text)
        brand = brand_result[0] if brand_result else ''
    else:
        brand = ''

    return description.text, price.get('content'), last_price, brand, url_image.img.get('src'), url


def webscraper(categories):
    """Start the process pool to execute the workers for each product

    Returns
    -------
        A Print streaming  the final details about the products te format will be a tuple
    """

    url_paths = []
    with open(os.path.join(sys.path[0], 'output.txt'), "r", encoding="utf8") as output_response:
        pages = output_response.read()

    soup = BeautifulSoup(pages, "html.parser")

    multipage_items = soup.find_all('li', class_="ui-search-layout__item")
    for items in multipage_items:
        url_paths.append(items.find('a').get('href'))

    singlepage_items = soup.find('ol', class_="items_container").find_all('a')
    for items in singlepage_items:
        url_paths.append(items.get('href'))

    logging.info("Web scraping started")

    with ProcessPoolExecutor(max_workers=16) as executor:
        for result in executor.map(webscrap_products, url_paths):
            print(result)

    logging.info("Web scraping finished")


if __name__ == "__main__":
    categories = category_reader()
    mercadolibre_webcrawler(categories)
    webscraper(categories)
