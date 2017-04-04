import os
from os.path import join
from collections import defaultdict

import wikitextparser as wtp
from joblib import Parallel, delayed

import Utils
import Config
from extractors import extract_bz2_dump


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


if __name__ == '__main__':
    fa_en_infobox_mapping()
