import os
import json
from os.path import join

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
        table_type : 1=normal, 2=first_col_is_counter, 3=is_columnar_predicate ( for special tables )
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
            predicate = [clean(data) if data else None for data in table_data[0][col_header_layer + 1:]]
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
            predicate = [clean(data) if data else None for data in table_data[0][col_header_layer:]]
        return subject, predicate, table_type, useful_table


def get_subject_predicate(subjects, predicates, table_type, row, col):
    if table_type == 2:
        predicate = predicates[row]
        subject = subjects[col]
    else:
        predicate = predicates[col]
        subject = subjects[row]
    return subject, predicate


def build_tuples(table, page_name, section_name):

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

            for row_index, row in enumerate(useful_table):
                for cell_index, cell in enumerate(row):
                    if cell:
                        for value in DataUtils.split_infobox_values(cell):
                            tuple_per_row = dict()
                            tuple_per_row['object'] = clean(value) if value else None
                            tuple_per_row['subject'], tuple_per_row['predicate'] = get_subject_predicate(subjects,
                                                                                                         predicates,
                                                                                                         table_type,
                                                                                                         row_index,
                                                                                                         cell_index)

                            # tuple_per_row['subject2'], tuple_per_row['predicate2'] = get_subject_predicate(subjects,
                            #                                                                                predicates,
                            #                                                                                table_type,
                            #                                                                                row_index,
                            #                                                                                cell_index)

                            # tuple_per_row['subject1'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                            # tuple_per_row['predicate1'] = section_name

                            if all(data_validation(data) for data in tuple_per_row.values()):
                                tuples.append(tuple_per_row)
                    else:
                        continue
        return tuples, extracted_tables

    else:
        return [], 0


def final_extractor(directory, filename):
    input_filename = join(directory, filename)
    tuples = list()
    page_num = all_tables_count = extracted_tables_count = 0

    for page in DataUtils.get_wikipedia_pages(filename=input_filename):
        page_num += 1
        parsed_page = DataUtils.parse_page(page)
        page_name = parsed_page.title.text
        try:
            text = parsed_page.revision.find('text').text
            wiki_text = wp.parse(text)
            for section in wiki_text.sections:
                for table in section.tables:
                    all_tables_count += 1
                    new_tuples, new_extracted_tables = build_tuples(table, page_name, section.title)
                    tuples.extend(new_tuples)
                    extracted_tables_count += new_extracted_tables
        except Exception as e:
            print('final extractor', e, page_name)

    DataUtils.save_json(Config.final_tuples_dir, filename, tuples)

    info = {
        'file name': filename,
        'all tables': all_tables_count,
        'extracted tables': extracted_tables_count,
        'page numbers': page_num,
        'tuples': len(tuples)
    }
    with open('info.txt', 'a') as info_file:
        info_file.write(json.dumps(info, ensure_ascii=False))


def multiprocess_extraction():
    directory = Config.extracted_pages_articles_dir['fa']
    filenames = os.listdir(directory)
    if filenames:
        Parallel(n_jobs=3)(delayed(final_extractor)(directory, filename)
                           for filename in filenames)


def get_wikitext_by_api(title):
    api = 'https://fa.wikipedia.org/w/api.php?'
    params = dict()
    params['action'] = 'query'
    params['format'] = 'json'
    params['prop'] = 'revisions'
    params['rvprop'] = 'content'
    params['titles'] = title

    result = requests.get(api, params=params)
    if result.status_code == 200:
        return result.json()['query']['pages'].popitem()[1].get('revisions')[0].get('*')
    else:
        return None


def table_extraction_bye_page_title(title):
    wiki_text = get_wikitext_by_api(title)
    if wiki_text:
        tuples = list()
        wiki_text = wp.parse(wiki_text)
        for section in wiki_text.sections:
            for table in section.tables:
                new_tuples = build_tuples(table, title, section.title)[0]
                tuples.extend(new_tuples)

        DataUtils.save_json(Config.final_tuples_dir, title, tuples)


if __name__ == '__main__':
    # multiprocess_extraction()
    # final_extractor(Config.extracted_pages_articles_dir['fa'], '4')
    # info = DataUtils.load_json(Config.final_tuples_dir, 'info')
    #
    # all_tables = extracted_tables=tuples=0
    # for item in info:
    #     all_tables+=item['all tables']
    #     extracted_tables+= item['extracted tables']
    #     tuples += item['tuples']
    # # with open('/home/nasim/Projects/kg/wiki-extractor/resources/refined/tuples/info.json')as f:
    # #     f.write('\n%d%d%d' %(all_tables, extracted_tables, tuples))
    # print(all_tables, extracted_tables, tuples)
    page_names = ['استان‌های_ایران', 'شهرستان‌های_ایران']
    page_names = ['تاریخچه_کنسول‌های_بازی‌های_ویدئویی_(نسل_چهارم)']
    for page_name in page_names:
        table_extraction_bye_page_title(page_name)
