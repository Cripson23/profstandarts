import json
import re
import supervenn
import bson
import pymorphy2
import collections
import matplotlib.pyplot as plt

from pymongo.mongo_client import MongoClient

db_name = 'characteristics'
connection_string = 'mongodb+srv://admin:O7jqDyag3J6P96Bk@cluster0.oqvlg.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'

class CharDictPSClassifier:
    def __init__(self):
        self.db = CharDictionary.connect_db(db_name)
        #self.tfs = self.get_filtered_tfs(direction, ps_stop_codes, education)

    # Получить список ПС по коду направления
    def get_ps_by_direction(self, direction_text):
        regx = bson.regex.Regex(f'^{direction_text}....')
        ps = self.db['prof_standarts'].find({'code': regx})
        return ps

    # Фильтр ПС, убрать ПС с заданными кодами
    @staticmethod
    def filter_ps_by_stop_codes(dir_ps, ps_stop_codes):
        result_ps = []
        for ps in dir_ps:
            if ps['code'] not in ps_stop_codes:
                result_ps.append(ps)

        return result_ps

    # Получить ПС по списку названий ПС
    @staticmethod
    def get_pses_by_ps_names_list(db, ps_list_names):
        pses = []
        for ps_name in ps_list_names:
            # !!! КОСТЫЛЬ НА ПРОФ 01.004
            if ps_name == '01.004':
                continue
            ps = db['prof_standarts'].find_one({'code': ps_name})
            pses.append(ps)
        return pses 

    # Получить ОТФы по списку проф. стандартов
    @staticmethod
    def get_otfs_by_ps_list(db, ps):
        otfs = []
        for p in ps:
            p_otf_list = db['otfs'].find({'ps_code': p['code']})
            for otf in p_otf_list:
                otfs.append(otf)
        return otfs

    # Фильтр ОТФ по образованию
    @staticmethod
    def get_otfs_edu_filter(otfs, filter_str):
        filtred_otfs = []

        for otf in otfs:
            edu_req = otf['edu_requirements']
            for edu in edu_req:
                if re.findall(rf'{filter_str}', edu.lower()):
                    filtred_otfs.append({
                        'ps_code': otf['ps_code'],
                        'code': otf['code']
                    })

        return filtred_otfs

    # Получить ТФы по ОТФам
    @staticmethod
    def get_tfs_by_otfs(db, otfs):
        tf_list = []
        for otf in otfs:
            tfs = db['tfs'].find({'ps_code': otf['ps_code'], 'otf_code': otf['code']})
            for tf in tfs:
                tf_list.append(tf)
        return tf_list

    def get_filtered_tfs(self, direction, ps_stop_codes, education):
        dir_ps = self.get_ps_by_direction(direction)
        if ps_stop_codes is not None:
            filter_ps_stop_codes = self.filter_ps_by_stop_codes(dir_ps, ps_stop_codes)
            otfs = self.get_otfs_by_ps_list(self.db, filter_ps_stop_codes)
        else:
            otfs = self.get_otfs_by_ps_list(self.db, dir_ps)
        otfs = self.get_otfs_edu_filter(otfs, education)
        tfs = self.get_tfs_by_otfs(self.db, otfs)
        return tfs


class CharDictionary:
    def __init__(self):
        self.db = self.connect_db(db_name)

    @staticmethod
    def connect_db(name):
        client = MongoClient(
            connection_string)
        return client[name]

    # Очистка текста
    @staticmethod
    def text_treatment(text):
        text = text.lower()
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'[\s]', ' ', text)
        text = text.split()
        text = list(filter(lambda x: len(x) > 3, text))
        if len(text) > 0:
            return text
        else:
            return ''

    # Лемматизация
    @staticmethod
    def lemmatize_sentence(words):
        morph = pymorphy2.MorphAnalyzer()
        res = []
        for word in words:
            p = morph.parse(word)[0]
            res.append(p.normal_form)

        return res

    # Получить словарь со словами по каждому ПС (без рассчётов)
    @staticmethod
    def get_words_dict_by_tfs(tfs, name):
        dictionary = {
            'name': name,
            'content': []
        }

        for tf in tfs:
            text = []
            if tf['labor_actions']:
                for labor_action in tf['labor_actions']:
                    text += CharDictionary.text_treatment(labor_action)

            if tf['required_skills']:
                for required_skills in tf['required_skills']:
                    text += CharDictionary.text_treatment(required_skills)

            if tf['required_knowledge']:
                for required_knowledge in tf['required_knowledge']:
                    text += CharDictionary.text_treatment(required_knowledge)
            text = CharDictionary.lemmatize_sentence(text)

            check_add = False
            for content_ps in dictionary['content']:
                if content_ps['ps_code'] == tf['ps_code']:
                    content_ps['words'] += text
                    check_add = True

            if not check_add:
                dictionary['content'].append({
                    'ps_code': tf['ps_code'],
                    'words': text
                })

        return dictionary


    @staticmethod
    def get_words_dict_by_fgos_dir(fgos_direction):
        db = CharDictionary.connect_db(db_name)
        fgos_dir = []

        regx = bson.regex.Regex(f'^{fgos_direction}......')
        fgos = db['fgos'].find({'code': regx})

        for f in fgos:
            text = []
            for uk in f['uk_codes']:
                uk_text = db['uk'].find_one({'code': uk})['name']
                text += CharDictionary.text_treatment(uk_text)
            if 'opk_codes' in f:
                for opk in f['opk_codes']:
                    opk_text = db['opk'].find_one({'code': opk})['name']
                    text += CharDictionary.text_treatment(opk_text)
            
            pses = CharDictPSClassifier.get_pses_by_ps_names_list(db, f['ps_codes'])
            otfs = CharDictPSClassifier.get_otfs_by_ps_list(db, pses)
            tfs = CharDictPSClassifier.get_tfs_by_otfs(db, otfs)
            
            for tf in tfs:
                if tf['labor_actions']:
                    for labor_action in tf['labor_actions']:
                        text += CharDictionary.text_treatment(labor_action)

                if tf['required_skills']:
                    for required_skills in tf['required_skills']:
                        text += CharDictionary.text_treatment(required_skills)

                if tf['required_knowledge']:
                    for required_knowledge in tf['required_knowledge']:
                        text += CharDictionary.text_treatment(required_knowledge)
            
            text = CharDictionary.lemmatize_sentence(text)

            fgos_dir.append({
                'fgos_code': f['code'],
                'words': text
            })
        return fgos_dir

    # Подсчёт коэффициентов для каждого токена (слова) в словаре
    @staticmethod
    def compute_dict(dict):
        for ps_content in dict['content']:
            count_words = collections.Counter(ps_content['words'])
            for i in count_words:
                count_words[i] = count_words[i] / float(len(ps_content['words']))
            ps_content['words'] = collections.OrderedDict(sorted(count_words.items(), key=lambda x: x[1], reverse=True))
        return dict

    # Формирование словаря
    def formation_dict(self, dict_name, tfs, fgos_direction):
        dictionary_tfs = CharDictionary.get_words_dict_by_tfs(tfs, dict_name)
        if fgos_direction is not None:
            dictionary_fgos = CharDictionary.get_words_dict_by_fgos_dir(fgos_direction)
            dictionary_tfs['content'] += dictionary_fgos

        dict_compute = CharDictionary.compute_dict(dictionary_tfs)
        if self.db['dictionaries'].find_one_and_replace({'name': dict_name}, dict_compute) is None:
            self.db['dictionaries'].insert_one(dict_compute)
    

    # Декомпозиция словаря характеристик
    def decomposition_dict_to_collection(self, dict_name):
        dict = self.db['dictionaries'].find_one({'name': dict_name})
        source_type = None
        source = None
        for elem in dict['content']:
            if 'ps_code' in elem:
                source_type = 'ps'
                source = 'ps_code'
            elif 'fgos_code' in elem:
                source_type = 'fgos'
                source = 'fgos_code'
            for word in elem['words']:
                self.db[dict_name].insert_one({
                    'source_type': source_type,
                    'source_code': elem[source],
                    'word': word,
                    'weight': round(elem['words'][word], 10)
                })

    """
    # ============ NOT USED METHODS
    # Получить все документы по названию коллекции
    def get_all_docs(self, collection_name):
        return self.db[collection_name].find()

    # Получить ТФы по ПС
    def get_tfs_by_ps(self, ps_code):
        tf_list = []

        tfs = self.db['tfs'].find({'ps_code': ps_code})
        for tf in tfs:
            tf_list.append(tf)

        return tf_list

    # Запись в файл
    @staticmethod
    def data_write(filename, data):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    """


class CharDictAnalysis:
    def __init__(self, dict_name):
        self.db = CharDictionary.connect_db(db_name)
        self.dict_name = dict_name

    # Получить список характеристик проф. стандарта
    def get_words_ps(self, dict_name, ps_code):
        dict = self.db['dictionaries'].find_one({'name': dict_name})
        for content in dict['content']:
            if 'ps_code' in content:
                if content['ps_code'] == ps_code:
                    return content['words']
            elif 'fgos_code' in content:
                if content['fgos_code'] == ps_code:
                    return content['words']

    # Получить название проф. стандарта по коду
    def get_ps_name_by_code(self, ps_code):
        ps = self.db['prof_standarts'].find_one({'code': ps_code})
        if not ps:
            fgos = self.db['fgos'].find_one({'code': ps_code})
            if not fgos:
                print(ps_code)
            else:
                print(fgos)
                return fgos['name']
        else:
            return ps['name']

    # сравнение всех проф. стандартов друг с другом
    def ps_all_comparison(self):
        result = []
        dict = self.db['dictionaries'].find_one({'name': self.dict_name})
        for idx, content in enumerate(dict['content']):
            for i in range(idx + 1, len(dict['content'])):
                type_content_code = None
                type_dict_content_code = None
                if 'ps_code' in content:
                    type_content_code = 'ps_code'
                elif 'fgos_code' in content:
                    type_content_code = 'fgos_code'
                if 'ps_code' in dict['content'][i]:
                    type_dict_content_code = 'ps_code'
                elif 'fgos_code' in dict['content'][i]:
                    type_dict_content_code = 'fgos_code'

                result.append(self.ps_comparison(content[type_content_code], dict['content'][i][type_dict_content_code]))
        return result

    # Сравнение двух проф. стандартов из одного словаря
    def ps_comparison(self, ps_one, ps_two):
        words_ps_one = self.get_words_ps(self.dict_name, ps_one)
        words_ps_two = self.get_words_ps(self.dict_name, ps_two)

        ps_one_name = self.get_ps_name_by_code(ps_one)
        ps_two_name = self.get_ps_name_by_code(ps_two)

        return {
            'first_ps': {
                'code': ps_one,
                'name': ps_one_name
            },
            'second_ps': {
                'code': ps_two,
                'name': ps_two_name
            },
            'result': CharDictAnalysis.get_percent_similarity(words_ps_one, words_ps_two)
        }

    # Процент соответствия двух наборов характеристик
    @staticmethod
    def get_percent_similarity(words_one, words_two):
        percent = 0
        for word in words_one:
            if word in words_two:
                percent += min(words_one[word], words_two[word])

        return percent * 100

    # Получить диаграмму множеств для двух ПС из словаря характеристик
    def get_venn(self, ps_code_one, ps_code_two):
        sets = []
        labels = []
        dict = self.db['dictionaries'].find_one({'name': self.dict_name})
        for content in dict['content']:
            if content['ps_code'] == ps_code_one or content['ps_code'] == ps_code_two:
                words_set = set(list(content['words']))
                sets.append(words_set)
                labels.append(content['ps_code'])

        plt.figure(figsize=(20, 10))
        supervenn.supervenn(sets, labels, rotate_col_annotations=True,
                            col_annotations_area_height=1.2, sets_ordering='minimize gaps',
                            min_width_for_annotation=180)
        plt.savefig(f'{self.dict_name}_{ps_code_one}_{ps_code_two}.png')

    # Получить общую диаграмму множеств для словаря характеристик
    def get_general_venn(self):
        sets = []
        labels = []
        dict = self.db['dictionaries'].find_one({'name': self.dict_name})
        for content in dict['content']:
            words_set = set(list(content['words']))
            sets.append(words_set)
            labels.append(content['ps_code'])
            
        plt.figure(figsize=(20, 10))
        supervenn.supervenn(sets, labels, rotate_col_annotations=True,
                            col_annotations_area_height=1.2, sets_ordering='minimize gaps',
                            min_width_for_annotation=180)
        plt.savefig(f'{self.dict_name}_all.png')


def main():
    # it_stop_ps = ['06.023', '06.040', '06.039', '06.005', '06.043',
    #               '06.045', '06.037', '06.038', '06.036', '06.007',
    #               '06.030', '06.021', '06.009', '06.002', '06.008',
    #               '06.029', '06.027', '06.026', '06.024', '06.020',
    #               '06.018', '06.010', '06.006']

    # classifier = CharDictPSClassifier()
    # tfs = classifier.get_filtered_tfs('06', it_stop_ps, 'высшее')

    # dict = CharDictionary()
    # dict.formation_dict('it', tfs, '09')

    # classifier = CharDictPSClassifier()
    # tfs = classifier.get_filtered_tfs('01', None, 'высшее')

    # dict = CharDictionary()
    # dict.formation_dict('ped', tfs, '44')

    # dict = CharDictionary()
    # dict.decomposition_dict_to_collection('ped')

    # analysis = CharDictAnalysis('it')
    # comparison = analysis.ps_all_comparison()
    # for comp in comparison:
    #     print(comp['first_ps']['code'], comp['first_ps']['name'], " | ", comp['second_ps']['code'], comp['second_ps']['name'], " | ", comp['result'])

    analysis = CharDictAnalysis('it')
    print(analysis.ps_comparison('06.003', '06.022'))

    comparison = analysis.ps_all_comparison()
    for comp in comparison:
        print(comp['first_ps']['code'], comp['first_ps']['name'], " | ", comp['second_ps']['code'], comp['second_ps']['name'], " | ", comp['result'])

    # analysis.get_venn('06.003', '06.022')
    # analysis.get_general_venn()

    # ped_classifier = CharDictPSClassifier('01', None, 'высшее')
    # CharDictionary('ped', ped_classifier.tfs, '44')

if __name__ == "__main__":
    main()
