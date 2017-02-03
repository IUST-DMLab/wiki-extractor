import os

from joblib import Parallel, delayed

import BZ2_dums_extractor
import Config


def main():
    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_articles_dump,
                                                  Config.extracted_pages_articles_dir)
    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_meta_current_dump,
                                                  Config.extracted_pages_meta_current_dir)

    extracted_pages_files = os.listdir(Config.extracted_pages_articles_dir)
    if extracted_pages_files:
        Parallel(n_jobs=-1)(delayed(BZ2_dums_extractor.extract_infoboxes)
                            (filename) for filename in extracted_pages_files)


if __name__ == '__main__':
    main()
