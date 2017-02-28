import os
from os.path import join
import Utils
import Config
from collections import OrderedDict, defaultdict
from extract_infobax_mapping import sql_create_table_command


def count_number_of_infoboxes():
    infobox_counters = dict()
    triple_counters = list()
    pages_path_with_infobox = os.listdir(Config.extracted_pages_path_with_infobox_dir)
    for filename in pages_path_with_infobox:
        filename = filename.replace('.json', '')
        pages_path = Utils.load_json(Config.extracted_pages_path_with_infobox_dir, filename)
        for infobox_name_type in pages_path:
            if infobox_name_type in infobox_counters:
                infobox_counters[infobox_name_type] += len(pages_path[infobox_name_type])
            else:
                infobox_counters[infobox_name_type] = len(pages_path[infobox_name_type])

    for infobox_name_type in infobox_counters:
        slash_index = infobox_name_type.index('/')
        template_name = infobox_name_type[:slash_index]
        template_type = infobox_name_type[slash_index+1:]
        triple_counters.append({'template_name': template_type,
                                'template_type': template_name,
                                'language': Utils.get_infobox_lang(template_name),
                                'count': infobox_counters[infobox_name_type]})

    Utils.save_json(Config.processed_data_dir, 'templates_counter',
                    sorted(triple_counters, key=lambda item: item['count'], reverse=True), sort_keys=False)

    order = ['template_name', 'template_type', 'language', 'count']
    sql_dump = Utils.get_wiki_templates_transcluded_on_pages_sql_dump(triple_counters, order)
    Utils.save_sql_dump(Config.processed_data_dir, 'templates_counter.sql', sql_dump)


def extract_infobox_properties():
    properties = dict()
    infobox_names = os.listdir(Config.extracted_pages_with_infobox_dir)
    for infobox_name in infobox_names:
        infobox_name_dir = join(Config.extracted_pages_with_infobox_dir, infobox_name)
        infobox_types = os.listdir(infobox_name_dir)
        for infobox_type in infobox_types:
            infobox_name_type_dir = join(Config.extracted_pages_with_infobox_dir, infobox_name, infobox_type)
            information_files = os.listdir(infobox_name_type_dir)
            for information_file in information_files:
                if Utils.is_infobox_file(information_file):
                    infoboxes_filename = information_file.replace('.json', '')
                    infoboxes = Utils.load_json(infobox_name_type_dir, infoboxes_filename)
                    for page_name in infoboxes:
                        for infobox in infoboxes[page_name]:
                            for p in infobox:
                                if p in properties:
                                    properties[p] += 1
                                else:
                                    properties[p] = 1
    Utils.save_json(Config.processed_data_dir, 'infobox_properties',
                    OrderedDict(sorted(properties.items(), key=lambda item: item[1], reverse=True)), sort_keys=False)


def aggregate_abstracts():
    abstracts_with_templates_filename = join(Config.processed_data_dir, 'abstracts_with_templates.txt')
    abstracts_without_templates_filename = join(Config.processed_data_dir, 'abstracts_without_templates.txt')
    abstracts_with_templates_file = open(abstracts_with_templates_filename, 'w+', encoding='utf8')
    abstracts_without_templates_file = open(abstracts_without_templates_filename, 'w+', encoding='utf8')
    for path, subdirs, files in os.walk(Config.extracted_pages_with_infobox_dir):
        for name in files:
            if 'abstracts' in name:
                name = name.replace('.json', '')
                abstracts = Utils.load_json(path, name)
                for page_name in abstracts:
                    abstracts_with_templates_file.write(page_name+'\n')
                    abstracts_with_templates_file.write(abstracts[page_name]+'\n\n')

    for path, subdirs, files in os.walk(Config.extracted_pages_without_infobox_dir):
        for name in files:
            if 'abstracts' in name:
                name = name.replace('.json', '')
                abstracts = Utils.load_json(path, name)
                for page_name in abstracts:
                    abstracts_without_templates_file.write(page_name+'\n')
                    abstracts_without_templates_file.write(abstracts[page_name]+'\n\n')

    abstracts_with_templates_file.close()
    abstracts_without_templates_file.close()


def aggregate_categories():
    categories_with_templates = dict()
    categories_without_templates = dict()
    for path, subdirs, files in os.walk(Config.extracted_pages_with_infobox_dir):
        for name in files:
            if 'categories' in name:
                name = name.replace('.json', '')
                categories = Utils.load_json(path, name)
                for page_name in categories:
                    categories_with_templates[page_name] = categories[page_name]

    for path, subdirs, files in os.walk(Config.extracted_pages_without_infobox_dir):
        for name in files:
            if 'categories' in name:
                name = name.replace('.json', '')
                categories = Utils.load_json(path, name)
                for page_name in categories:
                    categories_without_templates[page_name] = categories[page_name]

    Utils.save_json(Config.processed_data_dir, 'categories_with_templates', categories_with_templates)
    Utils.save_json(Config.processed_data_dir, 'categories_without_templates', categories_without_templates)


def count_triples():
    all_counter = 0
    triples_filenames = os.listdir(Config.extracted_infoboxes_dir)
    for filename in triples_filenames:
        with open(join(Config.extracted_infoboxes_dir, filename)) as fp:
            for l in fp:
                line = l.strip()
                if line == ',':
                    all_counter += 1

    print(all_counter)


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
    Utils.save_sql_dump(Config.processed_data_dir, table_name + '.sql', command)


def sql_insert_command(table_name, rows, key_order):
    values_name = '(`'+'`,`'.join(key_order[1:])+'`)'
    command = "INSERT INTO `%s`%s VALUES " % (table_name, values_name)
    for full_name_fa, infoboxes in rows.items():
        for full_name_en in infoboxes:
            type_fa, name_fa = Utils.find_get_infobox_name_type(full_name_fa)
            type_en, name_en = Utils.find_get_infobox_name_type(full_name_en)

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


if __name__ == '__main__':
    # count_number_of_infoboxes()
    # extract_infobox_properties()
    # aggregate_abstracts()
    # count_triples()
    # aggregate_categories()
    template_redirect_with_fa()
