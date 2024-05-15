from collections import deque
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
from pprint import pprint
from app_logger import get_logger
from files.gcp_utils.storage import GCS

logger = get_logger(__name__)


header = {'User-Agent':"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15"}
valid_paths = ('/find','/b')
url_blacklist_host = ('assets.ajio.com')
url_blacklist_path = ('/offers')
url_list=[]


def scrape_task(robots, bucket, gcs_raw_path,temp_storage):
    sitemap_queue = deque([])
    resp = requests.get(robots, headers=header)
    resp.raise_for_status()

    content_str = resp.content.decode('utf-8')

    for line in content_str.splitlines():
        if line.startswith("Sitemap:"):
            sitemap_queue.append(line.split(":",1)[1])

    logger.info("scraping sitemaps")
    while sitemap_queue:
        sitemap = sitemap_queue.popleft()
        resp = requests.get(sitemap, headers=header)
        resp.raise_for_status()
        # print(resp.content)
        # print(type(resp.content.decode('utf-8')))
        soup = BeautifulSoup(resp.content.decode('utf-8'), 'xml')
        sites = soup.find_all('loc')
        for site in sites:
            site_text = site.getText()
            if site_text.endswith('.xml'):
                sitemap_queue.append(site_text)
            else:
                url_list.append(site_text)
        # print(site.getText())

        # print(type(sites[0]))
    logger.info("scraping sitemaps completed")

    logger.info(f"URL List length - {len(url_list)}")
    
    logger.info("scraping URLs")
    ########################################
    #adjust these params before running
    n=len(url_list)
    batch_size = 250
    file_counter = 512
    start_point = file_counter*batch_size
    ########################################
    logger.info(f"params: {n},{batch_size},{start_point},{file_counter}")
    for i in range(start_point,start_point+n,batch_size):
        batch = url_list[i:i+batch_size]
        unscraped_urls =[] 
        error_url = []
        total_products = 0
        product = {}
        for url in batch:
            try:
                unscraped_urls.append(url)
                is_scraped, brand, product_list = find_product(url)
                if is_scraped:
                    unscraped_urls.pop()
            except:
                unscraped_urls.pop()
                error_url.append(url)
                brand, product_list = "", []
            total_products += len(product_list)
            product[brand] = product.get(brand,[])+product_list
        logger.info(f"Processed {len(batch)} URLs")
        logger.info(f"Total URL unscraped - {len(unscraped_urls)}")
        logger.info(f"Total URL error - {len(error_url)}")
        logger.info(f"Total products - {total_products}")
        file_name = f"products_{file_counter}.json"
        local_file = f"{temp_storage+file_name}"
        metadata_file = {
            "batch": batch,
            "unscraped_url": unscraped_urls,
            "error_url": error_url
        }
        logger.info(f"creating file - {file_name}")
        with open(local_file,"w") as pr_file:
            json.dump(product,pr_file,indent=4)
        with open(f"{temp_storage}metadata_{file_counter}.json","w") as m_file:
            json.dump(metadata_file,m_file)
        # GCS().save_file(local_file, gcs_file_name=f"{gcs_raw_path+file_name}", bucket_name=bucket)
        logger.info("creating file completed")
        file_counter+=1
        
    logger.info("scraping URLs completed")

    


def find_product(url):
    parsed_object = urlparse(url)
    product_list = []
    brand = parsed_object.hostname.split(".",1)[1].rsplit(".",1)[0]
    is_scraped=False
    if parsed_object.hostname not in url_blacklist_host and \
        not parsed_object.path.startswith(url_blacklist_path):
        page_type = 'category'
        if '/p/' in parsed_object.path:
            page_type = 'product'
        is_scraped=True
        resp = requests.get(url, headers=header)
        # print(resp)
        resp.raise_for_status()
        # print(resp.text)
        page = BeautifulSoup(resp.content, 'html.parser')
        divs = page.find_all("script")
        
        for div in divs:
            div_text = div.getText().strip()
            # print(div_text[:26])
            if div_text.startswith("window.__PRELOADED_STATE__"):
                data_json = json.loads(div_text.split("=",1)[1].strip()[:-1])
                products = data_json['grid']['entities']
                # print(len(products))
                if len(products) > 0:
                    for pr_id,pr_details in products.items():
                        product_json = {
                            "avg-rating": pr_details.get('averageRating',""),
                            "img-url": [im['url'] for im in pr_details.get('images') if im['imageType'] == 'PRIMARY' ][0] ,
                            "price": pr_details['price']['value'],
                            "was-price":pr_details['wasPriceData']['value'],
                            "name": pr_details['name'],
                            "url": pr_details['url'],
                            "product-id": pr_details['url'].rsplit("/",1)[1].split('_')[0],
                            "offer-price":pr_details["offerPrice"]['value'],
                            "rating-count":pr_details.get('ratingCount',""),
                            "cat1":pr_details['segmentNameText'],
                            "cat2":pr_details['verticalNameText'],
                            "cat3":pr_details['brickNameText'],
                            "brand":pr_details['brandTypeName']
                        }
                        product_list.append(product_json)
                else:
                    if page_type == 'product':
                        products = data_json['product']['productDetails']
                        product_json = {
                            "product-id": products['baseProduct'],
                            "avg-rating": products.get('averageRating',""),
                            "img-url": [im['url'] for im in products.get('images') if im['imageType'] == 'PRIMARY' ][0] ,
                            "price": products['price']['value'],
                            "was-price":products['wasPriceData']['value'],
                            "name": products['name'],
                            "url": products['url'],
                            "offer-price":min([x['maxSavingPrice'] for x in products["potentialPromotions"] if x['maxSavingPrice'] != 0]),
                            "rating-count":products.get('numberOfReviews',""),
                            "cat1":products['brickCategory'],
                            "cat2":products['brickSubCategory'],
                            "cat3":products['brickName'],
                            "brand":products['brandCode']
                        }
                        product_list.append(product_json)

        # pprint(product_list)
    # logger.info(url)
    # logger.info(len(product_list))
    return is_scraped,brand, product_list

# "bucket": "airflow-002",
#     "gcs_raw_path": "raw_data/",
#     "gcs_transformed_path":"transformed_data/",
#     "temp_storage": "../data/temp/"
scrape_task("https://www.ajio.com/robots.txt", "airflow-002", "raw_data/", "../data/temp/")
# print(find_product('https://www.trends.ajio.com/hi-attitude-printed-casual-shoes/p/450095084_fuchsia'))

    
