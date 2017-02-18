import os
from collections import defaultdict
from os.path import join
import Config
import Utils


def get_id_mapping(page_sql_file):
    all_records = Utils.get_sql_rows(page_sql_file)

    page_ids = dict()
    for record in all_records:
        for columns in record:
            page_id, page_namespace, page_title = columns[0], columns[1], columns[2]
            page_ids[page_id] = page_title

    Utils.save_json(Config.extracted_jsons, 'page_ids', page_ids)
    return page_ids


def get_lang_links(id_map):
    lang_links_en = dict()
    lang_links_ar = dict()

    all_records = Utils.get_sql_rows(Config.fawiki_latest_lang_links_dump)
    for record in all_records:
        for columns in record:
            ll_from, ll_lang, ll_title = columns[0], columns[1], columns[2]
            if ll_from in id_map:
                ll_from = id_map[ll_from]

                if ll_lang == 'en':
                    lang_links_en[ll_from] = ll_title
                elif ll_lang == 'ar':
                    lang_links_ar[ll_from] = ll_title

    Utils.save_json(Config.extracted_jsons, 'en_lang_links', lang_links_en)
    Utils.save_json(Config.extracted_jsons, 'ar_lang_links', lang_links_ar)


def get_redirect(id_map):
    redirects = dict()
    reverse_redirects = dict()

    all_records = Utils.get_sql_rows(Config.fawiki_latest_redirect_dump)
    for record in all_records:
        for columns in record:
            r_from, ns, r_title = columns[0], columns[1], columns[2]
            if r_from in id_map:
                r_from = id_map[r_from]

                if ns not in redirects:
                    redirects[ns] = dict()
                if ns not in reverse_redirects:
                    reverse_redirects[ns] = dict()
                if r_title not in reverse_redirects[ns]:
                    reverse_redirects[ns][r_title] = list()
                redirects[ns][r_from] = r_title
                reverse_redirects[ns][r_title].append(r_from)

    for ns in redirects:
        Utils.save_json(Config.extracted_redirects_dir, Utils.get_redirects_filename(ns), redirects[ns])
    for ns in reverse_redirects:
        Utils.save_json(Config.extracted_reverse_redirects_dir, Utils.get_redirects_filename(ns), reverse_redirects[ns])


def get_category_link(id_map):
    category_links = defaultdict(list)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_category_links_dump)
    for record in all_records:
        for columns in record:
            cl_from, cl_to = columns[0], columns[1]
            if cl_from in id_map:
                cl_from = id_map[cl_from]
                category_links[cl_from].append(cl_to)

    return category_links


def get_external_link(id_map):
    external_links = defaultdict(list)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_external_links_dump)
    for record in all_records:
        for columns in record:
            el_from, el_to = columns[1], columns[3]
            if el_from in id_map:
                el_from = id_map[el_from]
                external_links[el_from].append(el_to)

    return external_links


def get_wiki_link(id_map):
    wiki_links = defaultdict(list)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_page_links_dump)
    for record in all_records:
        for columns in record:
            pl_from, pl_title = columns[0], columns[2]
            if pl_from in id_map:
                pl_from = id_map[pl_from]
                wiki_links[pl_from].append(pl_title)

    return wiki_links


def extract_all():
    id_map = get_id_mapping(Config.fawiki_latest_page_dump)
    get_lang_links(id_map)
    get_redirect(id_map)

    page_ids_files = os.listdir(Config.extracted_page_ids_dir)
    for filename in page_ids_files:
        filename = filename.replace('.json', '')
        id_map = Utils.load_json(Config.extracted_page_ids_dir, filename)
        category_links = get_category_link(id_map)
        external_links = get_external_link(id_map)
        wiki_links = get_wiki_link(id_map)

        with_infobox_pages_path = Utils.load_json(Config.extracted_pages_path_with_infobox_dir, filename)

        for path in with_infobox_pages_path:
            absolute_resource_path = join(Config.extracted_pages_with_infobox_dir, path)
            Utils.save_json(absolute_resource_path, Utils.get_categories_filename(filename),
                            category_links, filter_dict=with_infobox_pages_path[path])
            Utils.save_json(absolute_resource_path, Utils.get_external_links_filename(filename),
                            external_links, filter_dict=with_infobox_pages_path[path])
            Utils.save_json(absolute_resource_path, Utils.get_wiki_links_filename(filename),
                            wiki_links, filter_dict=with_infobox_pages_path[path])

        without_infobox_pages_path = Utils.load_json(Config.extracted_pages_path_without_infobox_dir, filename)

        Utils.save_json(Config.extracted_pages_without_infobox_dir, Utils.get_categories_filename(filename),
                        category_links, filter_dict=without_infobox_pages_path)
        Utils.save_json(Config.extracted_pages_without_infobox_dir, Utils.get_external_links_filename(filename),
                        external_links, filter_dict=without_infobox_pages_path)
        Utils.save_json(Config.extracted_pages_without_infobox_dir, Utils.get_wiki_links_filename(filename),
                        wiki_links, filter_dict=without_infobox_pages_path)
