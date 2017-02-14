import os
import Utils
import Config


def count_number_of_infoboxes():
    infobox_counters = dict()
    pages_path_with_infobox = os.listdir(Config.extracted_pages_path_with_infobox_dir)
    for filename in pages_path_with_infobox:
        filename = filename.replace('.json', '')
        pages_path = Utils.load_json(Config.extracted_pages_path_with_infobox_dir, filename)
        for infobox_name_type in pages_path:
            if infobox_name_type in infobox_counters:
                infobox_counters[infobox_name_type] += len(pages_path[infobox_name_type])
            else:
                infobox_counters[infobox_name_type] = len(pages_path[infobox_name_type])

    Utils.save_json(Config.processed_data_dir, 'templates_counter', infobox_counters)


if __name__ == '__main__':
    count_number_of_infoboxes()
