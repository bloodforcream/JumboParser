import json
import ast
from datetime import datetime

import asyncio
import aiohttp
import scrapy
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

custom_spider_settings = {
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY': 2.0,

    'ROTATING_PROXY_LIST_PATH': 'checked_proxies.txt',
    'ROTATING_PROXY_PAGE_RETRY_TIMES': 2,
    'DOWNLOADER_MIDDLEWARES': {
        'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
    }
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19',
    'Content-Type': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
}

CITIES = {
    '3815 GK Amersfoort': 'uM0KYx4X0l0AAAFIDEEYwKrH',
    '5461 JS Veghell': '63UKYx4XPsEAAAFQvl1eATqA'
}

GRAPHQL_QUERY = """query product { product(sku: "%s") 
    {
        id: sku      
        subtitle: packSizeDisplay    
        title
        image 
        inAssortment 
        isAvailable 
        link  
        brand 
        category 
        thumbnails {image type}
        prices: price {
            price promoPrice pricePerUnit {price unit}}
        quantityOptions {
                maxAmount minAmount stepAmount unit}
        primaryBadge: primaryBadges {alt image}
        secondaryBadges {alt image} 
        promotions {
             tags {text}}}}"""

VALID_COUNTRIES = ['Germany', 'Netherlands', 'France', 'Hungary', 'Spain', 'Norway']


class JumboSpider(scrapy.Spider):
    name = 'JumboSpider'
    custom_settings = custom_spider_settings

    def start_requests(self):
        url = 'https://www.jumbo.com/producten/categorieen/diepvries/'

        for zipcode_city, city_uuid in CITIES.items():
            headers = {**HEADERS, 'Cookie': f'HomeStore={city_uuid}'}
            yield scrapy.Request(url=url, callback=self.get_amount_of_pages,
                                 meta={'headers': headers, 'zipcode_city': zipcode_city}, dont_filter=True)

    def get_amount_of_pages(self, response):
        xpath_pages_amount = '//*[@id="__layout"]/div/div[2]/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/ul[2]/li/text()'
        amount_of_pages = int(response.xpath(xpath_pages_amount).getall()[-1].strip())
        products_on_page = 25

        for num in range(amount_of_pages):
            page_url = f'{response.request.url}?offSet={num * products_on_page}'
            yield scrapy.Request(url=page_url, callback=self.get_products_urls, meta=response.meta, dont_filter=True)

    def get_products_urls(self, response):
        xpath_products_urls = '//*[@id="__layout"]/div/div[2]/div[2]/div[1]/div[2]/div[3]/div[2]/div[1]/div/div[1]/div/div[1]/a/@href'
        products_urls = response.xpath(xpath_products_urls).getall()

        for url in products_urls:
            product_url = f'https://www.jumbo.com{url}'
            yield scrapy.Request(url=product_url, callback=self.parse_product, meta=response.meta, dont_filter=True)

    def parse_product(self, response):
        soup = BeautifulSoup(response.text, 'lxml')

        description = soup.find_all(class_='jum-nutritional-info jum-product-info-item col-12')
        if description and 'Productomschrijving' in description:
            description = description[0].get_text().replace('Productomschrijving', '').strip()
        else:
            description = ''

        category = soup.find_all('div', attrs={'class': 'jum-product-characteristics jum-product-info-item col-12'})
        category = ast.literal_eval(category[0]['data-jum-product-details']).get('category').split(
            ', ') if category else []

        product_id = response.request.url.split('/')[-1]

        body = json.dumps({"query": GRAPHQL_QUERY % product_id})
        meta = {
            **response.meta,
            'description': description,
            'category': category
        }

        jumbo_api_url = 'https://www.jumbo.com/api/frontstore-api/'

        yield scrapy.Request(
            url=jumbo_api_url,
            callback=self.parse_product_internal_info,
            method='POST',
            body=body,
            headers=response.meta['headers'],
            meta=meta,
            dont_filter=True
        )

    def parse_product_internal_info(self, response):
        query_response = json.loads(response.text)

        data = query_response['data']['product']

        title = data['title']
        subtitle = data['subtitle'].replace(' ', '') or ''
        if subtitle:
            title = title.replace(subtitle, '').strip() + f", {subtitle}"

        marketing_tags = [[tag.get('text') for tag in prom.get('tags')]
                          for prom in data['promotions']]

        original_price = float(data['prices']['price'])
        current_price = float(data['prices']['promoPrice'] or original_price)
        sale_tag = str(current_price / original_price * 100) if current_price != original_price else "0"

        result = {
            'timestamp': datetime.strftime(datetime.now(), '%m/%d/%Y %H:%M:%S'),
            'RPC': data['id'],
            'url': 'https://www.jumbo.com' + data['link'],
            'title': title,
            'marketing_tags': marketing_tags[0] if marketing_tags else marketing_tags,
            'brand': data.get('brand'),
            'section': response.meta['category'],
            'price_data': {
                'current': current_price,
                'original': original_price,
                'sale_tag': f'Скидка {sale_tag}%'
            },
            'stock': {
                'in_stock': data.get('inAssortment'),
                'count': 0  # нет информации о количестве товара
            },
            'assets': {
                'main_image': data.get('image'),
                'set_images': [image.get('image') for image in data.get('thumbnails')],
                'view360': [],  # нет view360 ни на одном из товаров
                'video': []  # нет видео ни на одном из товаров
            },
            'metadata': {
                '__description': response.meta['description'],
            },
            'zipcode/city': response.meta['zipcode_city']
        }

        yield result


class GetProxySpider(scrapy.Spider):
    name = 'GetProxySpider'
    start_urls = ['https://free-proxy-list.net/']

    def parse(self, response):
        ips = response.xpath('//*[@id="proxylisttable"]/tbody/tr/td[1]/text()').getall()
        ports = response.xpath('//*[@id="proxylisttable"]/tbody/tr/td[2]/text()').getall()
        country_names = response.xpath('//*[@id="proxylisttable"]/tbody/tr/td[4]/text()').getall()

        proxies = []
        for index, country_name in enumerate(country_names):
            if country_name in VALID_COUNTRIES:
                proxies.append(f'{ips[index]}:{ports[index]}')
        asyncio.run(check_proxy(proxies))


async def check_proxy(proxies):
    url = 'https://httpbin.org/ip'

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[asyncio.create_task(fetch(session, url, proxy))
                                         for proxy in proxies])
        results = [result for result in results if result]

    with open('checked_proxies.txt', 'w+', encoding='utf8') as file:
        file.write('\n'.join(results))


async def fetch(session, url, proxy):
    try:
        async with session.get(url, proxy="http://" + proxy, timeout=4) as response:
            await response.text()
    except:
        return ''
    else:
        return proxy


def main():
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(GetProxySpider)
    process.crawl(JumboSpider)
    process.start()


if __name__ == '__main__':
    main()
