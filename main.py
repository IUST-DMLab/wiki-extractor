import os
from os.path import join
import json

from joblib import Parallel, delayed

import Wiki_sql_extractor
import BZ2_dums_extractor
import Config
import Utils


def main():
    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_articles_dump,
                                                  Config.extracted_pages_articles_dir)
    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_meta_current_dump,
                                                  Config.extracted_pages_meta_current_dir)

    # extracted_pages_files = os.listdir(Config.extracted_pages_articles_dir)
    # if extracted_pages_files:
    #     Parallel(n_jobs=4)(delayed(BZ2_dums_extractor.extract_bz2_dump)
    #                         (filename) for filename in extracted_pages_files)
    #     BZ2_dums_extractor.extract_bz2_dump('25')

    page_ids_files = os.listdir(Config.extracted_page_ids_dir)
    for filename in page_ids_files:
        id_map_filename = join(Config.extracted_page_ids_dir, filename)
        id_map_file = open(id_map_filename, encoding='utf8').read()
        id_map = json.loads(id_map_file, encoding='utf8')
        category_links = Wiki_sql_extractor.get_category_link(id_map)
        Utils.save_dict_to_json_file('a.json', category_links)


if __name__ == '__main__':
    main()
