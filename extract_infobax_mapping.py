import os
from os.path import join
import wikitextparser as wtp
from collections import defaultdict, OrderedDict

import Utils
import Config
from extractors import extract_bz2_dump
from joblib import Parallel, delayed


def extract_en_infobox(filename, fa_infoboxes_per_en_pages):
    input_filename = join(Config.extracted_en_pages_articles_dir, filename)

    mapping = defaultdict(list)
    for page in Utils.get_wikipedia_pages(filename=input_filename):
        parsed_page = Utils.parse_page(page)
        if parsed_page.title.text in fa_infoboxes_per_en_pages.keys():
            text = parsed_page.revision.find('text').text
            wiki_text = wtp.parse(text)

            templates = wiki_text.templates
            for template in templates:
                infobox_name, is_infobox = Utils.get_infobox_name_type(template.name)
                if is_infobox:
                    for fa_infobox in fa_infoboxes_per_en_pages[parsed_page.title.text]:
                        if infobox_name not in mapping[fa_infobox]:
                            mapping[fa_infobox].append(infobox_name)
    return mapping


def fa_en_infobox_mapping():
    """
    1. find pages in fa dump with fa_flag infoboxex
    2. find en pages of step1 pages from langlink results
    3. read en dump and extract template of pages in step2
    """

    en_lang_link = Utils.load_json(Config.extracted_lang_links_dir, Config.extracted_en_lang_link_filename)

    fa_infoboxes_per_pages = Utils.get_fa_infoboxes_per_pages()
    fa_infoboxes_per_en_pages = defaultdict(list)

    for fa_name, infoboxes in fa_infoboxes_per_pages.items():
        try:
            fa_infoboxes_per_en_pages[en_lang_link[fa_name.replace(' ', '_')]].extend(infoboxes)
        except KeyError:
            continue

    del en_lang_link
    del fa_infoboxes_per_pages

    extract_bz2_dump(Config.enwiki_latest_pages_articles_dump, Config.extracted_en_pages_articles_dir)

    extracted_en_pages_files = os.listdir(Config.extracted_en_pages_articles_dir)

    if extracted_en_pages_files:
        mapping_list = Parallel(n_jobs=-1)(delayed(extract_en_infobox)(filename, fa_infoboxes_per_en_pages)
                                           for filename in extracted_en_pages_files)

        mapping_result = defaultdict(list)
        for mapping in mapping_list:
            for fa_infobox_name in mapping:
                for en_infobox_name in mapping[fa_infobox_name]:
                    if en_infobox_name not in mapping_result[fa_infobox_name]:
                        mapping_result[fa_infobox_name].append(en_infobox_name)
        Utils.save_json(Config.extracted_infobox_mapping_dir, Config.extracted_infobox_mapping_filename, mapping_result)


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
    for fa_infobox, en_infoboxes in rows.items():
        full_name_fa, type_fa = Utils.get_infobox_name_type(fa_infobox)

        for en_infobox in en_infoboxes:
            full_name_en, type_en = Utils.get_infobox_name_type(en_infobox)

            command += "('%s','%s','%s','%s')," % (
                full_name_fa.replace('/', ' ', 1).replace("'", "''"),
                full_name_en.replace('/', ' ', 1).replace("'", "''"),
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
        'template_type_fa': 'varchar(500)',
        'template_type_en': 'varchar(500)',
    }
    indexing = "PRIMARY KEY (`id`),\n"

    key_order = ['id', 'template_full_name_fa', 'template_full_name_en', 'template_type_fa', 'template_type_en']

    ordered_table_structure = OrderedDict(sorted(table_structure.items(), key=lambda i: key_order.index(i[0])))

    command = sql_create_table_command(table_name, ordered_table_structure, indexing)

    command += sql_insert_command(table_name, rows, key_order)
    Utils.save_sql_dump(Config.refined_dir, table_name + '.sql', command)


if __name__ == '__main__':
    fa_en_infobox_mapping()
    fa_en_infobox_mapping_sql('wiki_template_mapping_extracted',
                              Utils.load_json(Config.extracted_infobox_mapping_dir,
                                              Config.extracted_infobox_mapping_filename))
