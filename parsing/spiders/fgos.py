import re
import scrapy
from parsing.pipelines import FgosPipeLine
from parsing.spiders import fgos
from parsing.items import FgosItem, UkItem, OpkItem

import logging

logging.basicConfig(filename='fgos.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('This will get logged to a file')


class StandartsListSpider(scrapy.Spider):
    name = "fgos"

    fgos_pipeline = FgosPipeLine()
    fgos_pipeline.open_spider(fgos)

    # Начало парсинга
    def start_requests(self):
        self.fgos_pipeline.direction = self.direction
        urls = []

        if self.direction == '09':
            urls = [
                'https://fgos.ru/fgos/fgos-09-03-01-informatika-i-vychislitelnaya-tehnika-929',
                'https://fgos.ru/fgos/fgos-09-03-02-informacionnye-sistemy-i-tehnologii-926',
                'https://fgos.ru/fgos/fgos-09-03-03-prikladnaya-informatika-922',
                'https://fgos.ru/fgos/fgos-09-03-04-programmnaya-inzheneriya-920'
            ]
        elif self.direction == '44':
            urls = [
                'https://fgos.ru/fgos/fgos-44-03-01-pedagogicheskoe-obrazovanie-121/',
                'https://fgos.ru/fgos/fgos-44-03-02-psihologo-pedagogicheskoe-obrazovanie-122/',
                'https://fgos.ru/fgos/fgos-44-03-03-specialnoe-defektologicheskoe-obrazovanie-123/',
                'https://fgos.ru/fgos/fgos-44-03-04-professionalnoe-obuchenie-po-otraslyam-124/',
                'https://fgos.ru/fgos/fgos-44-03-05-pedagogicheskoe-obrazovanie-s-dvumya-profilyami-podgotovki-125/'
            ]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
            'Host': 'fgos.ru'
        }
        for url in urls:
            yield scrapy.FormRequest(
                url=url,
                callback=self.start_parse_fgos,
                method='POST',
                headers=headers
            )
    
    def start_parse_fgos(self, response):
        uk_codes = []
        opk_codes = []
        ps_codes = []

        code_name_elem = response.css('.wpb_wrapper .wpb_text_column.wpb_content_element .wpb_wrapper h1::text').extract_first()[5:]

        code = re.match(r'\d\d.\d\d.\d\d', code_name_elem).group(0)
        name = re.sub(r'[\d.,]+', '', code_name_elem).strip()

        link = response.css('.wpb_wrapper p a::attr(href)').extract_first()
        order = response.css('.wpb_wrapper .wpb_text_column.wpb_content_element .wpb_wrapper h2::text').extract_first()
        registration = response.css('.wpb_wrapper .wpb_text_column.wpb_content_element .wpb_wrapper p::text').extract_first()

        uk_table_trs = response.css('.standart table')[1].css('tr')[1:]
        for idx, tr in enumerate(uk_table_trs):
            tds = tr.css('td')
            if len(tds) == 2:
                uk_group = tds[0].css('div::text').extract_first()
                uk_code_name = tds[1].css('div::text').extract_first()
            elif len(tds) == 1:
                uk_group = uk_table_trs[idx-1].css('td')[0].css('div::text').extract_first()
                uk_code_name = tds[0].css('div::text').extract_first()
            
            uk_code = re.match(r'^.*?\s', uk_code_name).group(0)[:-2]
            uk_name = re.search(r'\s.*', uk_code_name).group(0)[1:]
            
            uk_item = UkItem({
                'group': uk_group,
                'code': uk_code,
                'name': uk_name
            })
            self.fgos_pipeline.process_item(uk_item, fgos)

            uk_codes.append(uk_code)
        
        if self.direction == '44':
            opk_table_trs = response.css('.standart table')[2].css('tr')[1:]
            for tr in opk_table_trs:
                opk_group = tr.css('td .doc.left::text').extract_first()
                opk_code_name = tr.css('td .doc.justify::text').extract_first()
                opk_code = re.match(r'^.*?\s', opk_code_name).group(0)[:-2]
                opk_name = re.search(r'\s.*', opk_code_name).group(0)[1:]
                opk_item = OpkItem({
                    'direction': self.direction,
                    'group': opk_group,
                    'code': opk_code,
                    'name': opk_name
                })
                self.fgos_pipeline.process_item(opk_item, fgos)

                opk_codes.append(opk_code)

        if self.direction == '44':
            ps_table_trs = response.css('.standart table')[3].css('tr')[2:]
            for tr in ps_table_trs:
                ps_code = tr.css('td')[1].css('.doc.center::text').extract_first()
                ps_codes.append(ps_code)
        elif self.direction == '09':
            ps_table_trs = response.css('.standart table')[2].css('tr')[2:]
            for tr in ps_table_trs:
                ps_code = tr.css('td')[1].css('.doc.center::text').extract_first() 
                ps_codes.append(ps_code)

        if self.direction == '44':
            fgos_item = FgosItem({
                'code': code,
                'name': name,
                'link': link,
                'order': order,
                'registration': registration,
                'uk_codes': uk_codes,
                'opk_codes': opk_codes,
                'ps_codes': ps_codes
            })
        elif self.direction == '09':
            fgos_item = FgosItem({
                'code': code,
                'name': name,
                'link': link,
                'order': order,
                'registration': registration,
                'uk_codes': uk_codes,
                'ps_codes': ps_codes
            })

        self.fgos_pipeline.process_item(fgos_item, fgos)