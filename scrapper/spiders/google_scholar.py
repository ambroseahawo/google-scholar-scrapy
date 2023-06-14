import scrapy
from scrapper.items import ScrapperItem
from lxml import html


class GoogleScholarSpider(scrapy.Spider):
    name = "google_scholar"
    queries = ["technical indicators predict stock price", "momentum features for stock return factors", "geometric shapes technical indicators stock market", "price and volume data for stock market", "price based indicators stock prediciton"]
    pages = 1
    start_urls = ["https://scholar.google.com"]
    item = ScrapperItem()

    # https://pypi.org/project/scrapy-zyte-smartproxy/
    # https://scholar.google.com/scholar?start=0&q=ai&hl=en&as_sdt=0,5
    def parse(self, response):
        for search_query in self.queries:
            if len(search_query.split(" ")) > 1:
                full_search_query = str(search_query).replace(" ", "+")
                if self.pages > 1:
                    for page in range(self.pages):
                        page_number = page * 10
                        full_url = "{0}/scholar?start={1}&q={2}&hl=en&as_sdt=0,5".format(response.url, page_number, full_search_query)
                        yield response.follow(url=full_url, callback=self.extract_pdf_links, meta={'query': search_query})
                else:
                    full_url = "{0}/scholar?start=0&q={1}&hl=en&as_sdt=0,5".format(response.url, full_search_query)
                    yield response.follow(url=full_url, callback=self.extract_pdf_links, meta={'query': search_query})
            else:
                if self.pages > 1:
                    for page in range(self.pages):
                        page_number = page * 10
                        full_url = "{0}/scholar?start={1}&q={2}&hl=en&as_sdt=0,5".format(response.url, page_number, search_query)
                        yield response.follow(url=full_url, callback=self.extract_pdf_links, meta={'query': search_query})
                else:
                    full_url = "{0}/scholar?start=0&q={1}&hl=en&as_sdt=0,5".format(response.url, search_query)
                    yield response.follow(url=full_url, callback=self.extract_pdf_links, meta={'query': search_query})

    def extract_pdf_links(self, response):
        self.item["query"] = response.meta.get('query')
        div_sections = response.xpath('//div[@id="gs_res_ccl_mid"]//div[@class="gs_r gs_or gs_scl"]').getall()
        
        for section in div_sections:
            html_section = html.fromstring(section)
            section_url = html_section.xpath('//div//h3//a/@href')
            if section_url:
                self.item["url"] = section_url[0]
            else:
                self.item["url"] = ''

            section_title = " ".join(html_section.xpath('//div//h3//text()'))
            self.item["title"] = section_title

            section_citation = html_section.xpath('//div[@class="gs_fl"]//a[3]//text()')
            for wording in section_citation:
                if "Cited by " in wording:
                    self.item["citation"] = str(section_citation[0].split("by")[-1]).strip()
                else:
                    self.item["citation"] = ''

            yield self.item
