import scrapy
from ..items import CarsItem
import pygsheets
from pathlib import Path




class JijiSpider(scrapy.Spider):
    p  = Path('.')
    sv_file=list(p.glob('**/creds.json'))[0].resolve()
    #sv_file=r'C:\Users\HP\Dropbox\PC\Desktop\petroni\cars\cars\creds.json'
    gc=pygsheets.authorize(service_account_file=sv_file)

    current_sheet =gc.open_by_url('https://docs.google.com/spreadsheets/d/1EI7D_VvgsPuwQHdJf9WZNm6ixILChznyia5Kdd25y74/edit#gid=0')

    cwks=current_sheet[0]

    df=cwks.get_as_df()
    df=df.fillna('')
    #print(df)
    df=df.drop_duplicates(subset=['url'])
    urls=df['url'].tolist()
    
    name = "jiji"
    allowed_domains = ["jiji.co.ke"]
    start_urls = [
        "https://jiji.co.ke/api_web/v1/listing?slug=cars&init_page=true&page=1&webp=true&lsmid=1685052531225"
    ]

    download_delay = 0.3

    custom_settings = {"FEEDS": {"jiji.csv": {"format": "csv", "overwrite": True}}}

    def parse(self, response):
        data = response.json()

        for i in data["adverts_list"]["adverts"]:
            url = i["url"]

            yield scrapy.Request(
                url="https://jiji.co.ke" + url, callback=self.parse_page
            )

        if data["next_url"] != None:
            next_page = data["next_url"]
            yield scrapy.Request(url=next_page, callback=self.parse)

    def parse_page(self, response):

        items = CarsItem()

        price = response.xpath('//span[@itemprop="price"]/@content').get()

        name = response.xpath('//h1[@itemprop="name"]/div/text()').get().strip()
        
        mileage=response.xpath('//span[@itemprop="mileageFromOdometer"]/text()').get('')
        
        make= response.xpath('//div[@itemprop="brand"]/text()').get('')
        
        model= response.xpath('//div[@itemprop="model"]/text()').get('')
        year = response.xpath('//div[@itemprop="productionDate"]/text()').get('')
        
        fuel_type=response.xpath('//span[@itemprop="fuelType"]/text()').get('')
        
        drive_type=''
        leather_seats = ''
        
        sunroof = ''       
        content=response.xpath('//div[@class="b-advert-attribute__value"]/text()').getall()
        for i in content:
            i = i.strip()

            if "WD" in i:
                drive_type = i
                break

        attributes = response.xpath(
            '//div[@class="b-advert-attributes__tag"]/text()'
        ).getall()

        for ele in attributes:
            ele = ele.strip()

            if ("Leather" in ele) or ("Leather Seats" in ele):
                leather_seats = True

            elif "Sunroof" in ele:
                sunroof = True

            else:
                continue
        page_url=response.url.split('?')[0]
        items['url'] = response.url.split('?')[0]
        
        items['name'] = name
        items['price'] = int(price)
        items['mileage']=mileage
        items['make'] = make.strip()
        items['model'] = model.strip()
        items['year'] = year.strip()
        items['drive_type'] = drive_type
        items['leather_seats']=leather_seats
        items['fuel_type']=fuel_type
        items['sunroof'] = sunroof
        
        
        if str(page_url) in self.urls:
            print('duplicate url')
        else:
            yield items