import os
import json
from os.path import join
import re

import requests
import wikitextparser as wp
from joblib import Parallel, delayed

import Config
import DataUtils
from ThirdParty.WikiCleaner import clean


def cell_is_header(cell):
    try:
        return cell.string.strip().startswith('!')
    except AttributeError:
        return False


def row_is_header(row):
    return all(cell_is_header(cell) for cell in row if cell)


def row_has_header(row, col_header_layer):
    return any(cell_is_header(cell) for cell in row[col_header_layer:])


def bottom_header_checker(table_cells, table_rows_number, col_header_layer):
    """:return number of continuous rows from bottom of table that any of cell is header"""

    index = table_rows_number
    while index >= 0:
        if row_has_header(table_cells[index - 1], col_header_layer):
            index -= 1
        else:
            break
    return table_rows_number - index


def top_header_checker(table_cells, table_rows_number):
    """:return number of continuous rows from top of table that all cells are header
        return 3 for 3 header or more
    """
    for row_number in range(0, min(table_rows_number, 3)):
        if not row_is_header(table_cells[row_number]):
            return row_number
    return min(table_rows_number, 3)


def column_header_checker(table_cells):
    table_columns_number = len(table_cells[0])

    for col_number in range(0, min(table_columns_number, 3)):
        if not all(cell_is_header(table_cells[i][col_number]) for i in range(0, len(table_cells))):
            return col_number
    return min(table_columns_number, 3)


def headers_checker(table_cells):
    table_rows_number = len(table_cells)
    col_header_layer = column_header_checker(table_cells)
    top_header_layer = top_header_checker(table_cells, table_rows_number)
    bottom_header_layer = bottom_header_checker(table_cells, table_rows_number, col_header_layer)

    return col_header_layer, top_header_layer, bottom_header_layer


def fa_numbers_creator():
    fa_numbers = list()
    nums_1 = ["۰", "۱", "۲", "۳", "۴", "۵", "۶", "۷", "۸", "۹"]
    nums_2 = list()
    for first_number in nums_1:
        for second_number in nums_1:
            nums_2.append(first_number + second_number)

    for first_number in nums_1:
        for second_number in nums_2:
            fa_numbers.append(first_number + second_number)

    fa_numbers.extend(nums_2 + nums_1)
    return fa_numbers

FA_NUMBERS = fa_numbers_creator()


def first_col_is_counter(first_col):
    """:return  columnar header is No. or not"""
    return all(data in FA_NUMBERS for data in first_col)


def is_wiki_link(data):
    data = data.strip()
    return data.startswith('[[') and data.endswith(']]')


def has_wiki_link(cell):
    if cell.wikilinks:
        return True
    else:
        return False


def check_columnar_predicate(row_cells):
    """:return predicate is in cols or not"""
    return all(has_wiki_link(cell) for cell in row_cells)


def data_validation(data):
    invalid_data = ["", " ", None, "-", ".", "N/A", "n/a", "—"]
    if all(data != invalid_item for invalid_item in invalid_data):
        return True
    else:
        return False


def clean_subject(subject_cell):
    # todo: make decision
    if subject_cell and has_wiki_link(subject_cell):
        return clean(subject_cell.wikilinks[0].string)
    else:
        return None


def clean_object_predicate(cell):
    # todo: make decision
    if cell:
        if len(cell.wikilinks) <= 1:
            if is_wiki_link(cell.string):
                return clean(cell.string)
            else:
                return clean(cell.string, specify_wikilinks=False)
        else:
            if is_wiki_link(cell.string):
                return clean(cell.string, specify_wikilinks=False)
            else:
                return clean(cell.string, specify_wikilinks=False)
    else:
        return None


def prepare_subject_predicate(table_data, table_cells, top_header_layer, col_header_layer, bottom_header_layer):
    """
    :return
        subject: list of subjects
        predicate: list of predicates
        table_type : 1=first_col_is_counter, 2=normal, 3=is_columnar_predicate ( for special tables )
        useful_table: table without subjects and predicates
    """

    table_type = 0
    bottom_header_layer_index = len(table_data) - bottom_header_layer
    useful_table = [row[col_header_layer:] for row in
                    table_data[top_header_layer: bottom_header_layer_index]]

    if (top_header_layer, col_header_layer) == (1, 1):

        if first_col_is_counter(
                [row[0] for row in table_data[top_header_layer:bottom_header_layer_index]]):
            subject = [clean_subject(row[1]) for row in table_cells[top_header_layer:bottom_header_layer_index]]
            predicate = [clean(data, specify_wikilinks=False) if data else None for data in table_data[0][col_header_layer + 1:]]
            useful_table = [row[col_header_layer + 1:] for row in
                            table_data[top_header_layer: bottom_header_layer_index]]
            table_type = 1

        elif check_columnar_predicate(table_cells[0][col_header_layer:]):
            subject = [clean_subject(subject) for subject in table_cells[0][col_header_layer:]]
            predicate = [clean(row[0]) if row[0] else None for row in
                         table_data[top_header_layer:bottom_header_layer_index]]
            table_type = 2

        else:
            subject = [clean_subject(row[0]) for row in table_cells[col_header_layer:bottom_header_layer_index]]
            predicate = [clean(data, specify_wikilinks=False) if data else None for data in table_data[0][col_header_layer:]]
        return subject, predicate, table_type, useful_table


def get_subject_predicate(subjects, predicates, table_type, row, col):
    if table_type == 2:
        predicate = clean(predicates[row], specify_wikilinks=False)
        subject = subjects[col]
    else:
        predicate = clean(predicates[col], specify_wikilinks=False)
        subject = subjects[row]
    return subject, predicate


MAPPING = {
    'نام استان': 'foaf:name',
    'کد استان': 'fkgo:code',
    'مرکز': 'fkgo:capital',
    'تأسیس': 'fkgo:foundingDate',
    'مساحت': 'fkgo:areaTotal',
    'جمعیت': 'fkgo:populationTotal',
    'سال جمعیت': 'fkgo:populationAsOf',
    'شهرهای مهم': 'fkgo:city',
    'موقعیت در نقشه': 'fkgo:map',
    'مختصات مرکز استان': 'fkgo:capitalPosition',
    'رتبه در کشور': 'fkgo:populationTotalRankingInCountry',
    'نام شهرستان': 'foaf:name',
    'استان': 'fkgo:province',
    'سال سرشماری': 'fkgo:censusYear',
    'رتبه دراستان': 'fkgo:populationTotalRankingInProvince',
    'تاریخ تأسیس شهرستان': 'fkgo:foundingDate',
    'تغییر': 'fkgo:populationPercentageChange',
    'نام روستا': 'foaf:name',
    'دهستان': 'fkgo:ruralDistrict',
    'رتبه در دهستان': 'fkgo:populationTotalRankingInRuralDistrict',
    'نام': 'foaf:name',
    'کنسول': 'fkgo:picture',
    'قیمت درزمان عرضه': 'fkgo:launchPrice',
    'قیمت درزمان عرضه (USD)': 'fkgo:launchPrice',
    'زمان عرضه': 'fkgo:releaseDate',
    'رسانه': 'fkgo:computingMedia',
    'پرفروش‌ترین بازی‌ها': 'fkgo:bestSellingVideoGame',
    'سازگاری عقبرو': 'fkgo:backwardCompatibility',
    'امکانات': 'fkgo:accessory',
    'پردازشگر': 'fkgo:cpu',
    'حافظه': 'fkgo:ram',
    'ویدئو': 'fkgo:video',
    'سازنده': 'fkgo:manufacturer',
    'فروش جهانی': 'fkgo:gross',
    'صدا': 'fkgo:sound',
    'پروانه': 'fkgo:license',
    'نوع هسته': 'fkgo:kernelType',
    'هسته زبان برنامه‌نویسی': 'fkgo:kernelProgrammingLanguage',
    'پشتیبانی از ریسه در هسته': 'fkgo:kernelThreadSupport',
    'خانواده': 'fkgo:operatingSystemFamily',
    'قدیمی‌ترین نسخه منسوخ‌نشده': 'oldestNonEndOfLifeVersion',
    'انشعاب‌ها': 'projectFork',
    'معماری‌های مورد پشتیبانی': 'fkgo:supportedComputerArchitecture',
    'سیستم فایل‌های مورد پشتیبانی': 'fkgo:supportedFileSystem',
    'واسط گرافیکی به شکل پیش‌فرض': 'defaultGraphicalUserInterface',
    'مدیر بسته': 'fkgo:packageManager',
    'نحوه بروزرسانی': 'fkgo:updatingMethod',
    'ای‌پی‌آی‌های اصلی': 'applicationProgrammingInterface',
    'قدیمی‌ترین نسخه منسوخ نشده': 'fkgo:oldestNonEndOfLifeVersion'
}
def heuristic_check(tuple_per_row, population_counter):

    new_triple = None
    if 'جمعیت' in tuple_per_row['predicate']:
        int_part = re.search(r'\d+', tuple_per_row['predicate'])
        if int_part:
            new_triple = dict()
            counter = population_counter.setdefault(tuple_per_row['subject'], 0)
            year = int(int_part.group())
            new_triple['predicate'] = 'fkgo:populationAsOf' + str(counter+1)
            new_triple['object'] = year
            new_triple['subject'] = tuple_per_row['subject']
            new_triple['source'] = tuple_per_row['source']
            new_triple['version'] = tuple_per_row['version']
            predicate = 'fkgo:populationTotal' + str(counter+1)
            tuple_per_row['predicate'] = predicate
            population_counter[tuple_per_row['subject']] += 1

    if tuple_per_row['predicate'] == 'تاریخ تأسیسشهرستان':
        tuple_per_row['predicate'] = 'تاریخ تأسیس شهرستان'

    if tuple_per_row['predicate'] == 'سالمعرفی':
        tuple_per_row['predicate'] = 'سال معرفی'

    if tuple_per_row['predicate'] == 'موقعیت در نقشه ایران':
        tuple_per_row['predicate'] = 'موقعیت در نقشه'

    if tuple_per_row['predicate'] == 'http://fa.wikipedia.org/wiki/فهرست_پرفروش‌ترین_بازی‌های_ویدئویی':
        tuple_per_row['predicate'] = 'پرفروش‌ترین بازی‌ها'

    if tuple_per_row['predicate'] == 'http://fa.wikipedia.org/wiki/سازگاری_عقبرو':
        tuple_per_row['predicate'] = 'سازگاری عقبرو'

    if tuple_per_row['predicate'] == 'http://fa.wikipedia.org/wiki/واحد_پردازشگر_مرکزی':
        tuple_per_row['predicate'] = 'پردازشگر'

    if tuple_per_row['predicate'] == 'قیمت درزمان عرضه (http://fa.wikipedia.org/wiki/دلار_آمریکا )':
        tuple_per_row['predicate'] = 'قیمت درزمان عرضه'

    if tuple_per_row['predicate'] == 'http://fa.wikipedia.org/wiki/واحد_پردازش_گرافیکی':
        tuple_per_row['predicate'] = 'قیمت درزمان عرضه'

    if tuple_per_row['predicate'] not in MAPPING:
        print(tuple_per_row['predicate'])
    tuple_per_row['predicate'] = MAPPING.get(tuple_per_row['predicate'], tuple_per_row['predicate'])
    return tuple_per_row, new_triple, population_counter


def build_tuples(table, page_name, section_name, revision_id):
    image_names_types_in_fawiki = DataUtils.load_json(Config.extracted_image_names_types_dir,
                                                      Config.extracted_image_names_types_filename)

    extracted_tables = 0
    try:
        table_data = table.data()
        table_cells = table.cells()
    except (IndexError, ValueError) as e:
        # library exceptions
        print(e)
        return [], 0

    if table_data and table_cells:
        col_header_layer, top_header_layer, bottom_header_layer = headers_checker(table_cells)
        tuples = list()

        if (top_header_layer, col_header_layer) == (1, 1):
            extracted_tables += 1
            subjects, predicates, table_type, useful_table = prepare_subject_predicate(
                table_data, table_cells, top_header_layer, col_header_layer, bottom_header_layer)
            population_predicate_counter = dict()
            for row_index, row in enumerate(useful_table):
                for cell_index, cell in enumerate(row):
                    if cell:
                        for value in DataUtils.split_infobox_values(cell):
                            tuple_per_row = dict()
                            tuple_per_row['object'] = clean(value) if value else None
                            if DataUtils.is_image(tuple_per_row['object']):
                                tuple_per_row['object'] = DataUtils.clean_image_value(value, image_names_types_in_fawiki)

                            tuple_per_row['subject'], tuple_per_row['predicate'] = get_subject_predicate(subjects,
                                                                                                         predicates,
                                                                                                         table_type,
                                                                                                         row_index,
                                                                                                         cell_index)
                            tuple_per_row['module'] = "wiki_table_extractor"
                            tuple_per_row['source'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                            tuple_per_row['version'] = revision_id


                            # tuple_per_row['subject2'], tuple_per_row['predicate2'] = get_subject_predicate(subjects,
                            #                                                                                predicates,
                            #                                                                                table_type,
                            #                                                                                row_index,
                            #                                                                                cell_index)

                            # tuple_per_row['subject1'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                            # tuple_per_row['predicate1'] = section_name

                            tuple_per_row, hidden_tuple_per_row, population_predicate_counter = heuristic_check(
                                tuple_per_row, population_predicate_counter)
                            if all(data_validation(data) for data in tuple_per_row.values()):
                                tuples.append(tuple_per_row)
                                if hidden_tuple_per_row:
                                    tuples.append(hidden_tuple_per_row)
                    else:
                        continue
        return tuples, extracted_tables

    else:
        return [], 0


def get_wikitext_by_api(title):
    api = 'https://fa.wikipedia.org/w/api.php?'
    params = dict()
    params['action'] = 'query'
    params['format'] = 'json'
    params['prop'] = 'revisions'
    params['rvprop'] = 'content|ids'
    params['titles'] = title

    result = requests.get(api, params=params)
    if result.status_code == 200:
        return result.json()['query']['pages'].popitem()[1].get('revisions')[0].get('*'), \
               result.json()['query']['pages'].popitem()[1]['revisions'][0]['revid']
    else:
        return None


def table_extraction_bye_page_title(title):
    wiki_text, revision_id = get_wikitext_by_api(title)
    if wiki_text:
        tuples = list()
        wiki_text = wp.parse(wiki_text)
        for section in wiki_text.sections:
            for table in section.tables:
                new_tuples = build_tuples(table, title, section.title, revision_id)[0]
                tuples.extend(new_tuples)

        DataUtils.save_json(Config.wiki_table_tuples_dir, title, tuples)


if __name__ == '__main__':
    page_names = [
                  'استان‌های_ایران',
                  'شهرستان‌های_ایران',
                  'تاریخچه_کنسول‌های_بازی‌های_ویدئویی_(نسل_دوم)',
                  'تاریخچه_کنسول‌های_بازی‌های_ویدئویی_(نسل_سوم)',
                  'تاریخچه_کنسول‌های_بازی‌های_ویدئویی_(نسل_چهارم)',
                  'تاریخچه_کنسول‌های_بازی‌های_ویدئویی_(نسل_پنجم)',
                  'تاریخچه_کنسول‌های_بازی‌های_ویدئویی_(نسل_ششم)',
                  # 'رده‌بندی_ستارگان',
                  'شهرستان_بناب',
                  'مقایسه_اسمبلرها',
                  'مقایسه_سیستم‌عامل‌های_خانواده_بی‌اس‌دی',
                  'مقایسه_سیستم‌عامل‌های_متن‌باز',
                  'مقایسه_سیستم‌های_پرونده',
                  # 'یونیورسال_استودیوز'
                  ]

    for page_name in page_names:
        print(page_name)
        table_extraction_bye_page_title(page_name)
