import BZ2_dums_extractor
import Wiki_sql_extractor

import Config


def main():
    # BZ2_dums_extractor.extract_bz2_dump('25')
    # BZ2_dums_extractor.extract_all()
    # Wiki_sql_extractor.extract_all()

    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.enwiki_latest_pages_articles_dump,
                                                  Config.en_extracted_pages_articles_dir)


if __name__ == '__main__':
    main()
