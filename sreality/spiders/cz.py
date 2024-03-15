import scrapy
from scrapy.http import HtmlResponse
import boto3
from botocore.exceptions import ClientError
import os
class SrealitySpider(scrapy.Spider):
    name = "sreality"
    # start_urls = [
    #     "https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_type_cb=1&page=0&per_page=60"
    #
    # ]
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'sreality.csv',
        'DEPTH_LIMIT': 1,  # just this api call - no subsequent is needed
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': False,
        # robots.txt disallows to crawl - obey for testing purpose, for production need to register into sreality developer programme
    }
    if os.path.exists('sreality.csv'):
        os.remove('sreality.csv')

    # test_url_prefix = "https://www.sreality.cz/api"
    def start_requests(self):
        main_category=1
        while main_category<=2:
            # for url in range(0,38):
            for url in range(0,38):
                for sub_category in range(1,11):
                    yield scrapy.Request(
                        url=f"https://www.sreality.cz/api/cs/v2/estates?category_main_cb={main_category}&category_type_cb=1&building_condition={sub_category}&page={url}&per_page=60")


            main_category +=1
    def parse(self, response):
        jsonresponse = response.json()
        values = jsonresponse["filter"]["building_condition"]
        object_status=self.get_name_from_value(values)
        for estate in jsonresponse['_embedded']['estates']:
            yield {
                  "title": estate['name'],
                  "locality":estate['locality'],
                  "price":estate['price'],
                  "object_status":object_status,
                  "image": estate['_links']['images'][0]['href'],
                  "url":"https://www.sreality.cz/detail/prodej/dum/chalupa/loucna-pod-klinovcem-haj-/"+estate['_links']['self']['href'].split("/")[-1]
            }
    def get_name_from_value(self,input_value):
        data = [
            {"name": "Velmi dobrý", "value": "1"},
            {"name": "Dobrý", "value": "2"},
            {"name": "Špatný", "value": "3"},
            {"name": "Ve výstavbě", "value": "4"},
            {"name": "Developerské projekty", "value": "5"},
            {"name": "Novostavba", "value": "6"},
            {"name": "K demolici", "value": "7"},
            {"name": "Před rekonstrukcí", "value": "8"},
            {"name": "Po rekonstrukci", "value": "9"},
            {"name": "V rekonstrukci", "value": "10"}
        ]

        result = next((item["name"] for item in data if item["value"] == input_value), "Name not found")
        return result

    def closed(self, reason):
        self.logger.info('Spider closed:Hello %s' % reason)
        self.upload_file()

    def upload_file(self):
        file_name = 'sreality.csv'
        bucket = "reality-record"
        object_name = "sreality.csv"
        print("Testing")
        if object_name is None:
            object_name = os.path.basename(file_name)
        s3_client = boto3.client('s3',
                                 aws_access_key_id="AKIAZQ3DU2CCYENIH6ML",
                                 aws_secret_access_key="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")
        try:
            s3_client.head_object(Bucket=bucket, Key=object_name)
            s3_client.delete_object(Bucket=bucket, Key=object_name)
            print(f"Deleted existing object: {object_name}")
        except ClientError as e:
            # Ignore error if the object does not exist
            print("Error deleting object")
            if e.response['Error']['Code'] != '404':
                print(f"This is Error :{e}", )
                return False
        try:
            with open(file_name, "rb") as f:
                s3_client.upload_fileobj(f, bucket, object_name)
            print(f"Uploaded new file: {object_name}")
        except ClientError as e:
            print(f"Error: {e}")
            return False

        return True
