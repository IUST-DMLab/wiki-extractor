import wikitextparser as wp
import DataUtils
import Config
from ThirdParty.WikiCleaner import clean

from joblib import Parallel, delayed
import os
from os.path import join

from extractors import extract_bz2_dump


def cell_is_header(cell):
    try:
        return cell.string.strip().startswith('!')
    except AttributeError:
        return False


def row_is_header(row):
    return all(cell_is_header(cell) for cell in row)


def row_has_header(row, col_header_layer):
    return any(cell_is_header(cell) for cell in row[col_header_layer:])


def bottom_header_checker(table_cells, table_rows_number, col_header_layer):
    """:return number of continuous rows from bottom of table that any of cell is header"""

    index = table_rows_number
    while index >= 0:
        if row_has_header(table_cells[index-1], col_header_layer):
            index -= 1
        else:
            break
    return table_rows_number - index


def top_header_checker(table_cells, table_rows_number):
    """:return number of continuous rows from top of table that all cells are header
        return 3 for 3 header or more
    """

    top_header_layer = 0
    if table_rows_number > 0 and row_is_header(table_cells[0]):
        if table_rows_number > 1 and row_is_header(table_cells[1]):
            if table_rows_number > 2 and row_is_header(table_cells[2]):
                top_header_layer = 3
            else:
                top_header_layer = 2
        else:
            top_header_layer = 1

    return top_header_layer


def column_header_checker(table_cells):
    table_columns_number = len(table_cells[0])
    column_header_layer = 0

    if table_columns_number > 0 and \
            all(cell_is_header(table_cells[i][0]) for i in range(0, len(table_cells))):
        if table_columns_number > 1 and \
                all(cell_is_header(table_cells[i][1]) for i in range(0, len(table_cells))):
            if table_columns_number > 2 and \
                    all(cell_is_header(table_cells[i][2]) for i in range(0, len(table_cells))):
                column_header_layer = 3
            else:
                column_header_layer = 2
        else:
            column_header_layer = 1

    return column_header_layer


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
    # fa_numbers.extend(nums_1)
    for first_number in nums_1:
        for second_number in nums_1:
            nums_2.append(first_number + second_number)

    for first_number in nums_1:
        for second_number in nums_2:
            fa_numbers.append(first_number + second_number)

    fa_numbers.extend(nums_2 + nums_1)
    return fa_numbers


def first_col_is_counter(first_col):
    # todo: fa_number generator in main func
    fa_numbers = fa_numbers_creator()
    return all(data in fa_numbers for data in first_col)


def is_wiki_link(data):
    return data.startswith('[[') and data.endswith(']]')


def has_wiki_link(cell):
    if cell.wikilinks:
        return True
    else:
        return False


def is_columnar_predicate(row_cells):
    """:return predicate is in cols or not"""
    return all(has_wiki_link(cell) for cell in row_cells)


def prepare_subject_predicate(table_data, table_cells, top_header_layer, col_header_layer, bottom_header_layer):
    # todo: subject is col with the most wiki links
    if (top_header_layer, col_header_layer) == (1, 1):
        if first_col_is_counter([table_data[row][0] for row in table_data[top_header_layer:-bottom_header_layer]]):
            subject = [clean(table_data[row][1], specify_wikilinks=False) for row in table_data]
            predicate = [clean(data, specify_wikilinks=False) for data in table_data[0]]
        elif is_columnar_predicate(table_cells[0][col_header_layer:]):
            subject = [clean(data, specify_wikilinks=False) for data in table_data[0]]
            predicate = [clean(table_data[row][0], specify_wikilinks=False) for row in table_data]
        else:
            subject = [clean(table_data[row][0], specify_wikilinks=False) for row in table_data]
            predicate = [clean(data, specify_wikilinks=False) for data in table_data[0]]
        return subject, predicate


def build_tuples(table, page_name):
    table_data = table.data()
    table_cells = table.cells()
    # todo: start work from here :D
    # todo: fix subject predicate base on new function
    # todo: only subject that is entity is valid
    # todo: clean "", " " , ... from any data
    try:
        top_header_layer, col_header_layer, bottom_header_layer = headers_checker(table_cells)
        subject, predicates = prepare_subject_predicate(table_data, table_cells,
                                                        top_header_layer, col_header_layer, bottom_header_layer)
        tuples = list()
        if (top_header_layer, col_header_layer) == (1, 1):
            for row in table_data[1:]:
                for cell_index, cell in enumerate(row[1:]):
                    tuple_json = dict()
                    if cell:
                        tuple_json['object'] = clean(cell, specify_wikilinks=False)
                    else:
                        continue
                    tuple_json['subject'] = clean(row[0], specify_wikilinks=False)
                    tuple_json['predicate'] = clean(predicates[cell_index+1], specify_wikilinks=False)
                    tuple_json['page_name'] = page_name
                    tuples.append(tuple_json)
        return tuples
    except Exception as e:
        print('build tuple',e, page_name)
        return []


def multiprocess_extraction():
    directory = Config.extracted_pages_articles_dir['fa']
    filenames = os.listdir(directory)
    if filenames:
        Parallel(n_jobs=3)(delayed(final_extractor)(directory, filename)
                           for filename in filenames)


def final_extractor(directory, filename):
    input_filename = join(directory, filename)
    tuples = list()
    for page in DataUtils.get_wikipedia_pages(filename=input_filename):
        parsed_page = DataUtils.parse_page(page)
        page_name = parsed_page.title.text

        try:
            text = parsed_page.revision.find('text').text
            wiki_text = wp.parse(text)

            for table in wiki_text.tables:
                tuples.extend(build_tuples(table, page_name))

        except Exception as e:
            print('final extractor',e ,  page_name)
    print(len(tuples))
    DataUtils.save_json(Config.final_tuples_dir, filename, tuples)


def validate_data(data):
    # khali nabashe
    # tekrari ha
    # baraye inke perdicate satr ast ya sootoon tak tak nabinim kole satr ya soton ro bebinim
    # agar soton shomare bood sotone badesh subject
    # agar soton sal bood onvane safe subject

    pass


def clean_table_tuples():
    final_res = 0
    final_refined_res = 0
    invalid_tokens = ["", " "]
    for number in range(1, 101):
        invalid_tokens.append(number)
    fa_num = ["۰", "۱", "۲", "۳", "۴", "۵", "۶", "۷", "۸", "۹"]
    for first_number in fa_num:
        for second_number in fa_num:
            invalid_tokens.append(first_number + second_number)

    directory = Config.final_tuples_dir
    filenames = os.listdir(directory)

    for filename in filenames:
        data = DataUtils.load_json(directory, filename)
        final_res += len(data)
        for triple in data[:]:
            if set(invalid_tokens) & {triple['subject'], triple['predicate']}:
                data.remove(triple)
        final_refined_res += len(data)
        DataUtils.save_json(Config.final_refined_tuples_dir, filename, data)
    print('final_res', final_res)
    print('final_refined_res', final_refined_res)


if __name__ == '__main__':
    # multiprocess_extraction()
    # final_extractor(Config.extracted_pages_articles_dir['fa'], '8')
    # final_extractor(Config.resources_dir, 'taremi.txt')
    # data = DataUtils.load_json(Config.final_tuples_dir, '0')
    # print(len(data))
    # multiprocess_cleaning()
    clean_table_tuples()
