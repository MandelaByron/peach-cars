from cars.spiders.jiji import JijiSpider
from cars.spiders.carduka import CardukaSpider
from cars.spiders.eezycars import EezycarsSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import pandas as pd
from pathlib import Path
import gspread
import pygsheets
import datetime as dt

def upload_data():
    p  = Path('.')

    jiji_path=list(p.glob('**/jiji.csv'))[0].resolve()
    eezy_path=list(p.glob('**/eezycars.csv'))[0].resolve()
    carduka_path=list(p.glob('**/carduka.csv'))[0].resolve()
    print(jiji_path,eezy_path,carduka_path)


    df1=pd.read_csv(jiji_path)
    #df2=pd.read_csv(eezy_path)
    #df3=pd.read_csv(carduka_path)
    try:
            
            
            df3=pd.read_csv(carduka_path)
    except:
            df3=pd.DataFrame()


    try:
            df2=pd.read_csv(eezy_path)
    except:
            df2=pd.DataFrame()

    scraped_df=pd.concat([df1, df2,df3])
    scraped_df=scraped_df.fillna('')
    scraped_df=scraped_df.drop_duplicates(subset=['url'])
    
    scraped_df['timestamp'] = dt.datetime.now()
    
    scraped_df=scraped_df[[
            'url',
            'name',
            'price',
            'make',
            'model',
            'year',
            'mileage',
            'fuel_type',
            'drive_type',
            'sunroof',
            'leather_seats',
            'timestamp',
    ]]
    
    uploads=scraped_df.loc[:].values.tolist()
    
    sv_file=list(p.glob('**/creds.json'))[0].resolve()
    
    gc= gspread.service_account(filename=sv_file)
    #gc=pygsheets.authorize(service_account_file=sv_file)

    current_sheet =gc.open_by_url('https://docs.google.com/spreadsheets/d/1EI7D_VvgsPuwQHdJf9WZNm6ixILChznyia5Kdd25y74/edit#gid=0')
    
    cwks=current_sheet.get_worksheet(0)
    print(len(uploads))
    cwks.append_rows(uploads)

def main():
    settiings=get_project_settings()
    process = CrawlerProcess(settings=settiings)
    
    process.crawl(JijiSpider)
    process.crawl(CardukaSpider)
    process.crawl(EezycarsSpider)
    
    process.start()
    
if __name__ == '__main__':
    main()
    upload_data()