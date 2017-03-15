import os
from os.path import join
from collections import defaultdict

import Config
import Utils
import sql_generator


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


def mapping_sql():
    table_name = 'wiki_template_mapping_test6'
    table_structure = {
        'id': 'int NOT NULL AUTO_INCREMENT',
        'template_name_fa': 'varchar(500)',
        'template_name_en': 'varchar(500)',
        'approved': 'tinyint default NULL',
        'extraction_from': 'varchar(100)',
    }

    key_order = ['id', 'template_name_fa', 'template_name_en', 'approved', 'extraction_from']
    primary_keys = ['id']
    unique_keys = {'template_name_en_fa': ['template_name_fa', 'template_name_en']}

    ordered_table_structure = sql_generator.create_order_structure(table_structure, key_order)

    command = sql_generator.sql_create_table_command_generator(table_name, ordered_table_structure,
                                                               primary_key=primary_keys,
                                                               unique_key=unique_keys)
    sql_generator.execute_command_mysql(command)

    redirect_data = Utils.load_json(Config.extracted_redirects_dir, '10-redirects-with-fa')
    mapping_data = Utils.load_json(Config.extracted_infobox_mapping_dir, Config.extracted_infobox_mapping_filename)

    for redirect_from, redirects in redirect_data.items():
        for redirect_to in redirects:
            row = [{'template_name_fa': redirect_from, 'template_name_en': redirect_to, 'extraction_from': 'Redirect'}]
            query = sql_generator.insert_command(ordered_table_structure, table_name, key_order[1:3] + key_order[4:],
                                                 row)
            sql_generator.execute_command_mysql(query)

    for fa_infobox, en_infoboxes in mapping_data.items():
        for en_infobox in en_infoboxes:
            row = [{'template_name_fa': fa_infobox, 'template_name_en': en_infobox, 'extraction_from': 'Interlingual'}]
            query = sql_generator.insert_command(table_structure, table_name, key_order[1:3] + key_order[4:], row)
            sql_generator.execute_command_mysql(query)


if __name__ == '__main__':
    mapping_sql()
