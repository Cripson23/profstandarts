from parsing.pipelines import SimplePipeLine
from parsing.spiders import standarts_list
import re
import scrapy
from parsing.items import ProfStandartItem, ClassGroupsItem, EconomicActivitiesItem, OtfItem, TfItem

import logging

logging.basicConfig(filename='standarts_list.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('This will get logged to a file')


class StandartsListSpider(scrapy.Spider):
    name = "standarts_list"

    simple_pipeline = SimplePipeLine()
    simple_pipeline.open_spider(standarts_list)

    # Начало парсинга
    def start_requests(self):
        self.simple_pipeline.direction = self.direction
        urls = [
            'https://profstandart.rosmintrud.ru/obshchiy-informatsionnyy-blok/natsionalnyy-reestr-professionalnykh-standartov/reestr-professionalnykh-standartov/?RANGE_PROFACT=793&KIND_PROFACT=&set_filter=Y&PAGEN_1=1&sort=PROPERTY_REG_CODE&order=asc',
            'https://profstandart.rosmintrud.ru/obshchiy-informatsionnyy-blok/natsionalnyy-reestr-professionalnykh-standartov/reestr-professionalnykh-standartov/?RANGE_PROFACT=799&KIND_PROFACT=&set_filter=Y&PAGEN_1=1&sort=PROPERTY_REG_CODE&order=asc',
        ]
        data = {
            'PSCount': '100',
            'save': 'Показать'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
            'Host': 'profstandart.rosmintrud.ru'
        }
        if self.direction == 'all':
            for url in urls:
                yield scrapy.FormRequest(
                    url=url,
                    callback=self.start_parse_ps,
                    method='POST',
                    formdata=data,
                    headers=headers
                )
        elif self.direction == '01':
            yield scrapy.FormRequest(
                    url=urls[0],
                    callback=self.start_parse_ps,
                    method='POST',
                    formdata=data,
                    headers=headers
                )
        elif self.direction == '06':
            yield scrapy.FormRequest(
                    url=urls[1],
                    callback=self.start_parse_ps,
                    method='POST',
                    formdata=data,
                    headers=headers
                )
            
    
    def start_parse_ps(self, response):
        standarts_page_links = []

        # Получение ссылок
        for td in response.css('.listofitemps tbody tr td'):
            code = td.css('a::text').extract_first()
            if code is not None and re.match(r'\s*\d\d.\d\d\d\s*', code):
                standarts_page_links.append(td.css('a::attr(href)').extract_first())
        
        # Переход к парсингу каждого проф. стандарта
        
        for page in standarts_page_links:
            if page is not None:
                page = response.urljoin(page)
                request = scrapy.Request(page, callback=self.parse_prof_standart)
                request.meta['origin'] = page
                yield request
        
        '''
        page = response.urljoin(standarts_page_links[38])
        request = scrapy.Request(page, callback=self.parse_prof_standart)
        yield request
        '''
        
        
    # Парсинг одного проф. стандарта
    def parse_prof_standart(self, response):
        # Ссылка на источник
        origin = response.meta['origin']
        # Код
        code = response.css('td#OPDVPD center::text').extract_first()
        # Наименование вида проф. деятельности
        name = response.css('.h_title::text').extract_first()
        
        # Утверждение
        approved_lines = response.css('.tabContainer table')[0].css('tr')[0].css('td')[4].css('::text').extract()
        approved = self.get_ps_approved(approved_lines)

        # Рег. номер
        reg_number = int(response.css('.tabContainer table')[2].css('tr')[1].css('td')[1].css('center::text').extract_first())
        # Наименование вида профессиональной деятельности
        type_prof_activ = response.css('.tabContainer table tbody tr td font::text')[0].extract().strip()

        # Основная цель вида профессиональной деятельности
        purpose_prof_activ_lines = response.css('.tabContainer table tbody')[1].css('tr td::text').extract()
        purpose_prof_activ = self.get_purpose_pa(purpose_prof_activ_lines)

        # Группа занятий
        class_groups = self.get_class_groups(response)
        
        if class_groups:
            for group in class_groups:
                class_group_item = ClassGroupsItem(group)
                self.simple_pipeline.process_item(class_group_item, standarts_list)
            
            class_groups_codes = self.get_gc_codes(class_groups)
        else:
            class_groups = None
            class_groups_codes = None

        # Виды эконом. активности
        economic_activities = self.get_economic_activities(response, code)
        economic_activities_codes = None

        if economic_activities:
            for ea in economic_activities:
                economic_activities_item = EconomicActivitiesItem(ea)
                self.simple_pipeline.process_item(economic_activities_item, standarts_list)

            economic_activities_codes = self.get_ea_codes(economic_activities)
        else:
            economic_activities = None

        # Проф. стандарт
        prof_standart_item = ProfStandartItem({
                'origin': origin,
                'code': code,
                'name': name,
                'approved': approved,
                'reg_number': reg_number,
                'type_prof_activ': type_prof_activ,
                'purpose_prof_activ': purpose_prof_activ,
                'class_groups_codes': class_groups_codes,
                'economic_activities_codes': economic_activities_codes
        })
        
        self.simple_pipeline.process_item(prof_standart_item, standarts_list)

        otf_tf_links = []
        trs = response.css('.tabContainer table.separate')[1].css('tr')
        for tr in trs:
            tds = tr.css('td')
            idx = -1
            len_tds = len(tds)
            if len_tds == 6 or len_tds == 3:
                if len_tds == 6:
                    otf_link = tds[1].css('p a::attr(href)').extract_first()
                    tf_link = tds[3].css('p a::attr(href)').extract_first()
                    if otf_link is not None:
                        otf_tf_links.append({
                            'otf_link': otf_link,
                            'tf_links': [tf_link]
                        })
                        idx += 1

                if len_tds == 3:
                    tf_link = tds[0].css('p a::attr(href)').extract_first()
                    otf_tf_links[idx]['tf_links'].append(tf_link)
                    
        for otf_tf_link in otf_tf_links:
            page = response.urljoin(otf_tf_link['otf_link'])
            request = scrapy.Request(page, callback=self.parse_otf)
            request.meta['ps_code'] = code
            request.meta['tf_links'] = otf_tf_link['tf_links']
            yield request


    def parse_otf(self, response):
        ps_code = response.meta['ps_code']

        table_container_table = response.css('.table-container table')[0]
        code = table_container_table.css('tbody tr td')[3].css('center::text').extract_first()
        name = table_container_table.css('tbody tr td')[1].css('::text').extract_first()
        skill_level = table_container_table.css('tbody tr td')[5].css('center::text').extract_first()

        table_text_left = response.css('.table-container table.text-left tbody tr')
        job_titles = table_text_left.css('td')[1].css('::text').extract()
        
        edu_requirements = None
        experience_requirements = None
        special_conditions = None
        other_characteristics = None

        for idx, tr in enumerate(table_text_left):
            if idx < 2:
                continue

            tds = tr.css('td')

            column_text = tds[0].css('::text').extract_first()
            info_text = tds[1].css('::text').extract()

            if not info_text or info_text[0] == '-':
                    continue

            if re.search(r'Требования к образованию и обучению', column_text):
                edu_requirements = info_text
            elif column_text == 'Требования к опыту практической работы':
                experience_requirements = info_text
            elif re.search(r'Особые условия допуска к работе', column_text):
                special_conditions = info_text
            elif column_text == 'Другие характеристики':
                other_characteristics = info_text


        otf = OtfItem({
            'ps_code': ps_code,
            'name': name,
            'code': code,
            'skill_level': skill_level,
            'job_titles': job_titles,
            'edu_requirements': edu_requirements,
            'experience_requirements': experience_requirements,
            'special_conditions': special_conditions,
            'other_characteristics': other_characteristics
        })

        self.simple_pipeline.process_item(otf, standarts_list)

        tf_links = response.meta['tf_links']

        for link in tf_links:
            page = response.urljoin(link)
            request = scrapy.Request(page, callback=self.parse_tf)
            request.meta['otf_code'] = code
            request.meta['ps_code'] = ps_code
            yield request


    def parse_tf(self, response):
        ps_code = response.meta['ps_code']
        otf_code = response.meta['otf_code']

        table_container_table = response.css('.table-container table tbody tr td')
        code = table_container_table[3].css('center::text').extract_first()
        name = table_container_table[1].css('::text').extract_first()

        info_table = response.css('.text-left tbody tr')

        labor_actions = None
        required_skills = None
        required_knowledge = None
        other_characteristics = None

        for tr in info_table:
            tds = tr.css('td')

            column_text = tds[0].css('::text').extract_first()
            info_text = tds[1].css('::text').extract()

            if not info_text or info_text == '-':
                    continue

            if column_text == 'Трудовые действия':
                labor_actions = info_text
            elif column_text == 'Необходимые умения':
                required_skills = info_text
            elif column_text == 'Необходимые знания':
                required_knowledge = info_text
            elif column_text == 'Другие характеристики':
                other_characteristics = info_text
        
        tf = TfItem({
            'ps_code': ps_code,
            'otf_code': otf_code,
            'code': code,
            'name': name,
            'labor_actions': labor_actions,
            'required_skills': required_skills,
            'required_knowledge': required_knowledge,
            'other_characteristics': other_characteristics
        })

        self.simple_pipeline.process_item(tf, standarts_list)


    ####################################################################################
    # Получение групп занятий
    def get_class_groups(self, response):
        class_groups = []

        names = []
        okz_codes = []

        for tbody in response.css('.tabContainer table tbody'):
            tds = tbody.css('tr td')
            groups_id = tds.css('center').re(r'[0-9]{4}')
            for group_id in groups_id:
                    if group_id != '0000' and group_id != '':
                        okz_codes.append(int(group_id))

            for td in tds:
                width = td.css('::attr(width)').extract_first()
                if width == '39%' or width == '34%':
                    tds_text = td.css('::text').re(r'^[А-Я][А-Яа-я -]+')
                    for td_text in tds_text:
                        if td_text:
                            names.append(td_text.strip())
        
        for idx in range(len(okz_codes)):
            class_groups.append({
                'okz_code': okz_codes[idx],
                'name': names[idx]
            })

        return class_groups
    

    # Получение видов эконом. активности
    def get_economic_activities(self, response, prof_standart_item_code):
        economic_activities = []
        codes_okved = []
        ea_names = []

        for tbody in response.css('.tabContainer table tbody'):
            tds = tbody.css('tr td')
            ea_ids = tds.css('center::text').re(r'^[0-9]+\.[0-9.]+')
            for id in ea_ids:
                if id != prof_standart_item_code:
                    codes_okved.append(id)
            
            for td in tds:
                colspan = td.css('::attr(colspan)').extract_first()
                if colspan == '32':
                    ea_names.append(td.css('p::text').extract_first().strip())

        for idx in range(len(codes_okved)):
            economic_activities.append({
                'okved_code': codes_okved[idx],
                'name': ea_names[idx]
            })

        return economic_activities
    
    
    # Получение кодов эконом. активностей по массиву объектов
    def get_gc_codes(self, gc):
        codes = []
        for g in gc:
            codes.append(g['okz_code'])    
        return codes


    # Получение кодов эконом. активностей по массиву объектов
    def get_ea_codes(self, ea):
        codes = []
        for e in ea:
            codes.append(e['okved_code'])    
        return codes
    

    # Форматирование Утверждения
    def get_ps_approved(self, approved_lines):
        approved = ""
        for line in approved_lines:
            while "  " in line:
                line = line.replace("  ", " ")

            line = re.sub(r'[\n]','', line)
            approved += line

        return approved.strip()


    # Форматирование основной цели проф. деятельности
    def get_purpose_pa(self, purpose_lines):
        purpose_prof_activ = ""
        for idx, line in enumerate(purpose_lines):
                purpose_prof_activ += line.strip()
                if idx != len(purpose_lines)-1:
                    purpose_prof_activ += ' '
        return purpose_prof_activ