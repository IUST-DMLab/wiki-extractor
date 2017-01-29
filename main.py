import BZ2_dums_extractor
import Extractors
import Config


def main():
    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_articles_dump,
                                                  Config.extracted_pages_articles_dir)
    BZ2_dums_extractor.extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_meta_current_dump,
                                                  Config.extracted_pages_meta_current_dir)
    Extractors.extract_infoboxes()
    Extractors.extract_abstracts()
    Extractors.extract_ids()
    Extractors.extract_revision_ids()


if __name__ == '__main__':
    main()
