import csv
import json
import gzip
import Config
from os.path import join
from collections import defaultdict
from Utils import logging_file_operations, loggin_id_mapping_error


def dict_to_json(res, f_name):
    filename = join(Config.json_result_dir, f_name,)
    with open(filename, 'w') as fp:
        logging_file_operations(filename, 'Opened')
        json.dump(res, fp, ensure_ascii=False)
    logging_file_operations(filename, 'Closed')


def get_dump_rows(file_name, encoding='utf-8'):
    all_records = []

    with gzip.open(file_name, 'rt', encoding=encoding) as f:
        logging_file_operations(file_name, 'Opened')
        for line in f.readlines():

            if line.startswith('INSERT INTO '):
                all_records += find_records(line)
    logging_file_operations(file_name, 'Closed')
    return all_records


def find_records(line):
    records_str = line.partition('` VALUES ')[2]
    records_str = records_str.strip()[1:-2]
    records = records_str.split('),(')
    return records


def get_id_mapping(page_sql_file):
    all_records = get_dump_rows(page_sql_file)

    pages = {}
    for record in all_records:
        reader = csv.reader([record], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        for columns in reader:
            page_id, page_namespace, page_title = int(columns[0]), columns[1], columns[2]
            pages[page_id] = page_title
    dict_to_json(pages, 'id_title_map.json')
    return pages


def get_lang_links(id_map):
    id_not_found = 1
    all_records = get_dump_rows(Config.fawiki_latest_lang_links_dump)

    page_id_titles_en = defaultdict(list)
    page_id_titles_ar = defaultdict(list)
    for record in all_records:
        reader = csv.reader([record], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        for columns in reader:
            try:
                ll_from, ll_lang, ll_title = id_map[int(columns[0])], columns[1], columns[2]
            except KeyError:
                loggin_id_mapping_error(int(columns[0]), get_lang_links.__name__)
                ll_from, ll_lang, ll_title = int(columns[0]), columns[1], columns[2]
                id_not_found += 1

            if ll_lang == 'en':
                page_id_titles_en[ll_from].append(ll_title)
            elif ll_lang == 'ar':
                page_id_titles_ar[ll_from].append(ll_title)

    dict_to_json(page_id_titles_ar, 'ar_lang_link.json')
    dict_to_json(page_id_titles_en, 'en_lang_link.json')
    print("id not found ----->> %d" % id_not_found)
    return page_id_titles_en, page_id_titles_ar


def get_redirect(id_map):
    id_not_found = 0
    all_records = get_dump_rows(Config.fawiki_latest_redirect_dump)

    redirects = defaultdict(list)

    for record in all_records:
        reader = csv.reader([record], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        for columns in reader:
            try:
                r_from, r_title = id_map[int(columns[0])], columns[2]
            except KeyError:
                loggin_id_mapping_error(int(columns[0]), get_redirect.__name__)
                r_from, r_title = int(columns[0]), columns[2]
                id_not_found += 1

            redirects[r_from].append(r_title)
    dict_to_json(redirects, 'redirect.json')
    print("id not found ----->> %d" % id_not_found)
    return redirects


def get_category_link(id_map):
    id_not_found = 0
    all_records = get_dump_rows(Config.fawiki_latest_category_links_dump, encoding='ISO-8859-1')

    category_links = defaultdict(list)

    for record in all_records:
        reader = csv.reader([record], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        for columns in reader:
            try:
                cl_from, cl_to = id_map[int(columns[0])], columns[1]
            except KeyError:
                loggin_id_mapping_error(int(columns[0]), get_category_link.__name__)
                cl_from, cl_to = int(columns[0]), columns[1]
                id_not_found += 1

            category_links[cl_from].append(cl_to)
    dict_to_json(category_links, 'category.json')
    print("id not found ----->> %d" % id_not_found)
    return category_links


def get_external_link(id_map):
    id_not_found = 0
    all_records = get_dump_rows(Config.fawiki_latest_external_links_dump)

    external_links = defaultdict(list)

    for record in all_records:
        reader = csv.reader([record], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        for columns in reader:
            try:
                el_from, el_to = id_map[int(columns[1])], columns[3]
            except KeyError:
                loggin_id_mapping_error(int(columns[0]), get_external_link.__name__)
                el_from, el_to = int(columns[1]), columns[3]
                id_not_found += 1

            external_links[el_from].append(el_to)
    dict_to_json(external_links, 'external_links.json')
    print("id not found ----->> %d" % id_not_found)
    return external_links


def get_wiki_link(id_map):
    id_not_found = 0
    all_records = get_dump_rows(Config.fawiki_latest_page_links_dump, encoding='ISO-8859-1')

    wiki_links = defaultdict(list)

    for record in all_records:
        reader = csv.reader([record], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        for columns in reader:
            try:
                pl_from, pl_title = id_map[int(columns[0])], columns[2]
            except KeyError:
                loggin_id_mapping_error(int(columns[0]), get_wiki_link.__name__)
                pl_from, pl_title = int(columns[0]), columns[2]
                id_not_found += 1

            wiki_links[pl_from].append(pl_title)
    dict_to_json(wiki_links, 'wiki_links.json')
    print("id not found ----->> %d" % id_not_found)
    return wiki_links


def main():
    id_map = get_id_mapping(Config.fawiki_latest_page_dump)
    get_lang_links(id_map)
    get_redirect(id_map)
    get_category_link(id_map)
    get_external_link(id_map)
    get_wiki_link(id_map)


if __name__ == '__main__':
    main()
