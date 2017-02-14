import os
from os.path import join
import Utils
import Config


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
            triple_counters.append({'template_name': template_name,
                                    'template_type': template_type,
                                    'count': infobox_counters[infobox_name_type]})

    Utils.save_json(Config.processed_data_dir, 'templates_counter', triple_counters)


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
    Utils.save_json(Config.processed_data_dir, 'infobox_properties', properties)


if __name__ == '__main__':
    # count_number_of_infoboxes()
    extract_infobox_properties()
