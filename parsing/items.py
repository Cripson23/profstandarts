# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ProfStandartItem(scrapy.Item):
    origin = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    approved = scrapy.Field()
    reg_number = scrapy.Field()
    type_prof_activ = scrapy.Field()
    purpose_prof_activ = scrapy.Field()
    class_groups_codes = scrapy.Field()
    economic_activities_codes = scrapy.Field()


class ClassGroupsItem(scrapy.Item):
    okz_code = scrapy.Field()
    name = scrapy.Field()


class EconomicActivitiesItem(scrapy.Item):
    okved_code = scrapy.Field()
    name = scrapy.Field()


class OtfItem(scrapy.Item):
    ps_code = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    skill_level = scrapy.Field()
    job_titles = scrapy.Field()
    edu_requirements = scrapy.Field()
    experience_requirements = scrapy.Field()
    special_conditions = scrapy.Field()
    other_characteristics = scrapy.Field()


class TfItem(scrapy.Item):
    ps_code = scrapy.Field()
    otf_code = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    labor_actions = scrapy.Field()
    required_skills = scrapy.Field()
    required_knowledge = scrapy.Field()
    other_characteristics = scrapy.Field()


class FgosItem(scrapy.Item):
    code = scrapy.Field()
    name = scrapy.Field()
    link = scrapy.Field()
    order = scrapy.Field()
    registration = scrapy.Field()
    uk_codes = scrapy.Field()
    opk_codes = scrapy.Field()
    ps_codes = scrapy.Field()


class UkItem(scrapy.Item):
    group = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()


class OpkItem(scrapy.Item):
    direction = scrapy.Field()
    group = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()