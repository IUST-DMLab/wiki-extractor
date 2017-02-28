import os
import json
from os.path import join
import wikitextparser as wtp
from collections import defaultdict, OrderedDict

import Utils
import Config
from Utils import first_slash_splitter
from BZ2_dums_extractor import extract_wikipedia_bz2_dump


def extract_en_infobox(filename, fa_infoboxes, mapping):

    input_filename = join(Config.en_extracted_pages_articles_dir, filename)

    for page in Utils.get_wikipedia_pages(filename=input_filename):
        parsed_page = Utils.parse_page(page)

        if parsed_page.title.text in fa_infoboxes.keys():
            text = parsed_page.revision.find('text').text
            wiki_text = wtp.parse(text)

            templates = wiki_text.templates
            for template in templates:
                infobox_name, infobox_type = Utils.find_get_infobox_name_type(template.name)
                if infobox_name and infobox_type:
                    for path in fa_infoboxes[parsed_page.title.text]:
                        en_mapping = infobox_name + '/' + infobox_type
                        if en_mapping not in mapping[path]:
                            mapping[path].append(en_mapping)
    return mapping


def fa_en_infobox_mapping():
    """
    1. find pages in fa dump with fa_flag infoboxex
    2. find en pages of step1 pages from langlink results
    3. read en dump and extract template of pages in step2
    """

    fa_infoboxes = Utils.get_fa_infoboxes_names()

    with open(Utils.get_information_filename(Config.extracted_jsons, Config.extracted_en_lang_link_file_name)) as f:
        en_lang_link = json.load(f)

    en_titles = defaultdict(list)
    for path, names in fa_infoboxes.items():
        for name in names:
            try:
                en_titles[en_lang_link[name]].append(path)
            except KeyError:
                continue

    del en_lang_link

    extract_wikipedia_bz2_dump(Config.enwiki_latest_pages_articles_dump, Config.en_extracted_pages_articles_dir)

    extracted_en_pages_files = os.listdir(Config.en_extracted_pages_articles_dir)

    mapping_result = defaultdict(list)
    for page in extracted_en_pages_files:
        extract_en_infobox(page, en_titles, mapping_result)

    Utils.save_json(Config.extracted_jsons, Config.extracted_infobox_mapping, mapping_result)


def sql_create_table_command(table_name, columns, index):

    command = "DROP TABLE IF EXISTS `%s`;\n" % table_name
    command += "CREATE TABLE `%s` (\n " % table_name

    for key, value in columns.items():
        command += "`%s` %s,\n" % (key, value)

    command += index
    command = command[:-2] + ')CHARSET=utf8;\n'
    return command


def sql_insert_command(table_name, rows, key_order):
    values_name = '(`'+'`,`'.join(key_order[1:])+'`)'
    command = "INSERT INTO `%s`%s VALUES " % (table_name, values_name)
    for full_name_fa, infoboxes in rows.items():
        for full_name_en in infoboxes:
            type_fa, name_fa = first_slash_splitter(full_name_fa)
            type_en, name_en = first_slash_splitter(full_name_en)

            command += "('%s','%s','%s','%s', '%s', '%s')," % (
                full_name_fa.replace('/', ' ', 1).replace("'", "''"),
                full_name_en.replace('/', ' ', 1).replace("'", "''"),
                name_fa.replace("'", "''"),
                name_en.replace("'", "''"),
                type_fa.replace("'", "''"),
                type_en.replace("'", "''"),
            )

    command = command[:-1] + ";"
    return command


def fa_en_infobox_mapping_sql(table_name, rows):
    table_structure = {
        'id': 'int NOT NULL AUTO_INCREMENT',
        'template_full_name_fa': 'varchar(1000)',
        'template_full_name_en': 'varchar(1000)',
        'template_name_fa': 'varchar(500)',
        'template_name_en': 'varchar(500)',
        'template_type_fa': 'varchar(500)',
        'template_type_en': 'varchar(500)',
    }
    indexing = "PRIMARY KEY (`id`),\n"

    key_order = ['id', 'template_full_name_fa', 'template_full_name_en', 'template_name_fa',
                 'template_name_en', 'template_type_fa', 'template_type_en']

    ordered_table_structure = OrderedDict(sorted(table_structure.items(), key=lambda i: key_order.index(i[0])))

    command = sql_create_table_command(table_name, ordered_table_structure, indexing)

    command += sql_insert_command(table_name, rows, key_order)
    Utils.save_sql_dump(Config.processed_data_dir, table_name + '.sql', command)


if __name__ == '__main__':
    # fa_en_infobox_mapping()
    fa_en_infobox_mapping_sql('wiki_extracted_template_mapping',
                              Utils.load_json(Config.extracted_jsons, Config.extracted_infobox_mapping))
