import os
from collections import OrderedDict, defaultdict
from os.path import join

import Config
import Utils
from extract_infobax_mapping import sql_create_table_command


def aggregate_abstracts():
    abstracts_with_templates_filename = join(Config.refined_dir, 'abstracts_with_templates.txt')
    abstracts_without_templates_filename = join(Config.refined_dir, 'abstracts_without_templates.txt')
    abstracts_with_templates_file = open(abstracts_with_templates_filename, 'w+', encoding='utf8')
    abstracts_without_templates_file = open(abstracts_without_templates_filename, 'w+', encoding='utf8')
    for path, subdirs, files in os.walk(Config.extracted_with_infobox_dir):
        for name in files:
            if 'abstracts' in name:
                abstracts = Utils.load_json(path, name)
                for page_name in abstracts:
                    abstracts_with_templates_file.write(page_name+'\n')
                    abstracts_with_templates_file.write(abstracts[page_name]+'\n\n')

    for path, subdirs, files in os.walk(Config.extracted_without_infobox_dir):
        for name in files:
            if 'abstracts' in name:
                abstracts = Utils.load_json(path, name)
                for page_name in abstracts:
                    abstracts_without_templates_file.write(page_name+'\n')
                    abstracts_without_templates_file.write(abstracts[page_name]+'\n\n')

    abstracts_with_templates_file.close()
    abstracts_without_templates_file.close()


def aggregate_categories():
    categories_with_templates = dict()
    categories_without_templates = dict()
    for path, subdirs, files in os.walk(Config.extracted_with_infobox_dir):
        for name in files:
            if 'categories' in name:
                categories = Utils.load_json(path, name)
                for page_name in categories:
                    categories_with_templates[page_name] = categories[page_name]

    for path, subdirs, files in os.walk(Config.extracted_without_infobox_dir):
        for name in files:
            if 'categories' in name:
                categories = Utils.load_json(path, name)
                for page_name in categories:
                    categories_without_templates[page_name] = categories[page_name]

    Utils.save_json(Config.refined_dir, 'categories_with_templates', categories_with_templates)
    Utils.save_json(Config.refined_dir, 'categories_without_templates', categories_without_templates)


def template_redirect_with_fa():
    redirects = Utils.load_json(Config.extracted_redirects_dir, Utils.get_redirects_filename('10'))
    with_fa_redirects = defaultdict(list)

    for redirect_from, redirect_to in redirects.items():
        if Utils.is_ascii(redirect_from):
            if Utils.without_en_chars(redirect_to):
                with_fa_redirects[redirect_to].append(redirect_from)
        elif Utils.without_en_chars(redirect_from):
            if Utils.is_ascii(redirect_to):
                with_fa_redirects[redirect_from].append(redirect_to)

    Utils.save_json(Config.extracted_redirects_dir, '10-redirects-with-fa', with_fa_redirects)
    fa_en_infobox_mapping_sql('wiki_template_mapping_from_redirects', with_fa_redirects)


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
    Utils.save_sql_dump(Config.refined_dir, table_name + '.sql', command)


def sql_insert_command(table_name, rows, key_order):
    values_name = '(`'+'`,`'.join(key_order[1:])+'`)'
    command = "INSERT INTO `%s`%s VALUES " % (table_name, values_name)
    for full_name_fa, infoboxes in rows.items():
        for full_name_en in infoboxes:
            type_fa, name_fa = Utils.get_infobox_name_type(full_name_fa)
            type_en, name_en = Utils.get_infobox_name_type(full_name_en)

            full_name_fa = full_name_fa.replace('_', ' ').replace("'", "''") if\
                full_name_fa is not None else full_name_fa
            full_name_en = full_name_en.replace('_', ' ').replace("'", "''") if\
                full_name_en is not None else full_name_en
            name_fa = name_fa.replace("'", "''") if name_fa is not None else full_name_fa
            name_en = name_en.replace("'", "''") if name_en is not None else full_name_en
            type_fa = type_fa.replace("'", "''") if type_fa is not None else 'NULL'
            type_en = type_en.replace("'", "''") if type_en is not None else 'NULL'

            command += "('%s','%s','%s','%s', '%s', '%s')," % (
                full_name_fa,
                full_name_en,
                name_fa,
                name_en,
                type_fa,
                type_en
            )

    command = command[:-1] + ";"
    return command

import pymysql
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASS = ''
MYSQL_DB = 'kg'


def sql_new_insert_command(table_name, rows, key_order):
    values_name = '(`'+'`,`'.join(key_order[1:])+'`)'
    command = "INSERT INTO `%s`%s VALUES " % (table_name, values_name)
    for redirect_from, redirects in rows.items():
        for redirect_to in redirects:
            template_name_fa = redirect_from.replace('_', ' ').replace("'", "''")
            template_name_en = redirect_to.replace('_', ' ').replace("'", "''")

            command += "('%s','%s')," % (
                template_name_fa,
                template_name_en,
            )
    command = command[:-1] + ";"
    return command


def db_connection():
    try:
        connection = pymysql.connect(host=MYSQL_HOST,
                                     user=MYSQL_USER,
                                     password=MYSQL_PASS,
                                     db=MYSQL_DB,
                                     charset='utf8',
                                     )
        return connection
    except Exception as e:
        return None

from datetime import datetime
def wiki_template_mapping_sql_generator():
    time = datetime.now()

    table_name = 'wiki_template_mapping'
    table_structure = {
        'id': 'int NOT NULL AUTO_INCREMENT',
        'template_name_fa': 'varchar(500)',
        'template_name_en': 'varchar(500)',
        'approved': 'tinyint default NULL',
    }
    indexing = "PRIMARY KEY (`id`),\n" \
               "UNIQUE KEY (`template_name_fa`, `template_name_en`),\n"

    key_order = ['id', 'template_name_fa','template_name_en', 'approved']

    ordered_table_structure = OrderedDict(sorted(table_structure.items(), key=lambda i: key_order.index(i[0])))

    command = sql_create_table_command(table_name, ordered_table_structure, indexing)
    sql_command_executor(command)

    redirect_data = Utils.load_json('/home/nasim/Projects/kg/wiki-extractor/resources/extracted/jsons/redirects', '10-redirects-with-fa')
    mapping_data = Utils.load_json('/home/nasim/Projects/kg/wiki-extractor/resources/extracted/jsons', 'mappings_v4.1')

    print("________________redirects______________")
    for redirect_from , redirects in redirect_data.items():
        for redirect_to in redirects:
            query = sql_one_insert_command(table_name, key_order[1:3], redirect_from, redirect_to)
            sql_command_executor(query)
            command += query + '\n'

    print("________________mapping______________")
    for fa_infobox, en_infoboxes in mapping_data.items():

        for en_infobox in en_infoboxes:
            query = sql_one_insert_command(table_name, key_order[1:3], fa_infobox, en_infobox)
            sql_command_executor(query)
            command += query + '\n'


    Utils.save_sql_dump('/home/nasim/Projects/kg/wiki-extractor/resources/processed_data', table_name + '.sql', command)
    print("\n\n")
    print(datetime.now() - time)


def sql_command_executor(query):
    connection = db_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
        except Exception as e:
            print(e)

        finally:
            connection.close()


def sql_one_insert_command(table_name, key_order, template_name_fa, template_name_en):
    values_name = '(`'+'`,`'.join(key_order)+'`)'
    command = "INSERT INTO `%s`%s VALUES " % (table_name, values_name)
    template_name_fa = template_name_fa.replace('_', ' ').replace("'", "''")
    template_name_en = template_name_en.replace('_', ' ').replace("'", "''")

    command += "('%s','%s')," % (
        template_name_fa,
        template_name_en,
    )
    command = command[:-1] + ";"
    return command


if __name__ == '__main__':
    wiki_template_mapping_sql_generator()
