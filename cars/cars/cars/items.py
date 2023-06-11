# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CarsItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    name = scrapy.Field()
    model = scrapy.Field()
    make = scrapy.Field()
    mileage = scrapy.Field()
    price = scrapy.Field()
    drive_type = scrapy.Field()
    year = scrapy.Field()
    fuel_type = scrapy.Field()
    leather_seats = scrapy.Field()
    sunroof = scrapy.Field()
