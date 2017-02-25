import os
import json
from os.path import join
import wikitextparser as wtp
from collections import defaultdict

import Utils
import Config
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


def main():
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


if __name__ == '__main__':
    main()
