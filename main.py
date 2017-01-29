import Extractors


def main():
    Extractors.extract_wikipedia_page()
    Extractors.extract_infoboxes()
    Extractors.extract_abstracts()
    Extractors.extract_ids()
    Extractors.extract_revision_ids()


if __name__ == '__main__':
    main()
