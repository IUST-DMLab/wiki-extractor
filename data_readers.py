import os
from os.path import join
import Utils
import Config
from collections import OrderedDict


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


if __name__ == '__main__':
    # count_number_of_infoboxes()
    # extract_infobox_properties()
    # aggregate_abstracts()
    # count_triples()
    aggregate_categories()
