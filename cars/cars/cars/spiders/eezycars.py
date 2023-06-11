import scrapy
from ..items import CarsItem
import pygsheets
from pathlib import Path



class EezycarsSpider(scrapy.Spider):
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
    link_urls=df['url'].tolist()
    
    name = "eezycars"
    allowed_domains = ["eezycars.co.ke"]
    start_urls = ["https://www.eezycars.co.ke/auction/index?AuctionGrid_page=1"]

    custom_settings = {"FEEDS": {"eezycars.csv": {"format": "csv", "overwrite": True}}}

    def parse(self, response):
        urls = []
        links = response.xpath('//a[@class="car_link"]/@href').getall()
        for link in links:
            urls.append("https://www.eezycars.co.ke" + link)

        names = response.xpath('//a[@class="car_link"]/text()').getall()

        mileage = response.xpath(
            '//td[@class="vehicle_mileage group_a"]/text()'
        ).getall()

        year = response.xpath('//td[@class="vehicle_year"]/text()').getall()

        price = response.xpath('//td[@class="bid_price"]/text()').getall()
        data = zip(urls, names, mileage, year, price)

        for url, name, mil, year, price in list(data):
            price = price.replace(",", "").strip()
            items = {
                "url": url,
                "name": name,
                "mil": mil,
                "year": year,
                "price": int(price),
            }

            yield scrapy.Request(url=url, callback=self.parse_page, meta=items)
        next_page = response.xpath('(//li[@class="next"]/a/@href)[1]').get()

        if next_page != None:
            yield scrapy.Request(
                "https://www.eezycars.co.ke" + next_page, callback=self.parse
            )

    def parse_page(self, response):
        items = CarsItem()

        data = response.meta
        
        leather_seats=''
        sunroof=''
        
        attrs=response.xpath('//table[@class="table-striped table-condensed"]/tr/td[2]/text()').getall()
        #print(attrs)
        for i in attrs:
            i = i.strip()
            #print(i)
            if ('Leather' in i) or ('leather' in i):
                leather_seats=True
                print(i)
              
            elif('Sunroof' in i) or ('Sun' in i):
                sunroof=True
                
            else:
                continue
        
        items['url'] = response.url
        items['name'] = data['name']
        items['mileage']= data['mil']
        
        items['year'] = data['year']
        items['leather_seats']=leather_seats
        items['sunroof'] = sunroof
        items['price'] = data['price']
        
        #yield items
       
        if response.url in self.link_urls:
            
            print('duplicate url,skipping---')
        else:
            yield items
        
        
        