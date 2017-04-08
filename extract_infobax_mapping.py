import os
from collections import defaultdict
from os.path import join

import wikitextparser as wtp
from joblib import Parallel, delayed

import Config
import DataUtils
import extractors


def extract_en_infobox(filename, fa_infoboxes_per_en_pages):
    input_filename = join(Config.extracted_pages_articles_dir['en'], filename)

    mapping = defaultdict(list)
    for page in DataUtils.get_wikipedia_pages(filename=input_filename):
        parsed_page = DataUtils.parse_page(page)
        if parsed_page.title.text in fa_infoboxes_per_en_pages.keys():
            text = parsed_page.revision.find('text').text
            wiki_text = wtp.parse(text)

            templates = wiki_text.templates
            for template in templates:
                infobox_name, is_infobox = DataUtils.get_infobox_name_type(template.name)
                if is_infobox:
                    for fa_infobox in fa_infoboxes_per_en_pages[parsed_page.title.text]:
                        if infobox_name not in mapping[fa_infobox]:
                            mapping[fa_infobox].append(infobox_name)
    return mapping


def fa_en_infobox_mapping():
    """
    1.
    2.
    3. read en dump and extract template of pages in step2
    """

    fa_infoboxes_per_en_pages = DataUtils.get_fa_infoboxes_per_en_pages()

    extractors.extract_bz2_dump('en')

    extracted_en_pages_files = os.listdir(Config.extracted_pages_articles_dir['en'])

    if extracted_en_pages_files:
        mapping_list = Parallel(n_jobs=-1)(delayed(extract_en_infobox)(filename, fa_infoboxes_per_en_pages)
                                           for filename in extracted_en_pages_files)

        mapping_result = defaultdict(list)
        for mapping in mapping_list:
            for fa_infobox_name in mapping:
                for en_infobox_name in mapping[fa_infobox_name]:
                    if en_infobox_name not in mapping_result[fa_infobox_name]:
                        mapping_result[fa_infobox_name].append(en_infobox_name)
        DataUtils.save_json(Config.extracted_infobox_mapping_dir, Config.extracted_infobox_mapping_filename,
                            mapping_result)


if __name__ == '__main__':
    fa_en_infobox_mapping()
