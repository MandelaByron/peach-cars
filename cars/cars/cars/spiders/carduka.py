import scrapy
import pandas as pd
from ..items import CarsItem
import pygsheets
from pathlib import Path
class CardukaSpider(scrapy.Spider):
    p  = Path('.')
    sv_file=list(p.glob('**/creds.json'))[0].resolve()

    gc=pygsheets.authorize(service_account_file=sv_file)

    current_sheet =gc.open_by_url('https://docs.google.com/spreadsheets/d/1EI7D_VvgsPuwQHdJf9WZNm6ixILChznyia5Kdd25y74/edit#gid=0')

    cwks=current_sheet[0]

    df=cwks.get_as_df()
    df=df.fillna('')
    #print(df)
    df=df.drop_duplicates(subset=['url'])
    link_urls=df['url'].tolist()
    
    name = "carduka"
    allowed_domains = ["carduka.com"]
    start_urls = ["https://www.carduka.com/cars-on-auction?page=1"]

    custom_settings = {"FEEDS": {"carduka.csv": {"format": "csv", "overwrite": True}}}

    def parse(self, response):
        links = response.xpath('//div[@class="card featured-card"]/a/@href').getall()

        for i in links:
            yield scrapy.Request(url=i, callback=self.parse_page)

        next_page = response.xpath(
            '//a[@class="page-link" and @rel="next"]/@href'
        ).get()
        if next_page != None:
            yield scrapy.Request(url=next_page, callback=self.parse)

    def parse_page(self, response):

        items = CarsItem()
        df = pd.read_html(response.text)[0]

        make_ = df.loc[0, :].values.tolist()
        d_make = {make_[i]: make_[i + 1] for i in range(0, len(make_), 2)}
        make = d_make["Make"]

        model_ = df.loc[1, :].values.tolist()
        d_model = {model_[i]: model_[i + 1] for i in range(0, len(model_), 2)}
        model = d_model["Model"]

        name = response.xpath('//div[@class="card-body pt-3"]/h4/text()').get()
        
        year = name.split(',')[0]
        price = response.xpath('//div[@class="container text-center mt-2 pt-2"]/h3/text()').get()
        price= price.replace('Kshs.','').replace(',','').strip()
        items['url'] = response.url
        items['name']=name
        items['year'] = year
        items['make'] = make
        items['model'] = model
        
        items['price'] = int(price)  
        #yield items    
        if response.url in self.link_urls:
            print('duplicate url')
        else:
            yield items
        
        
        
