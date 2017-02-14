import BZ2_dums_extractor
import Wiki_sql_extractor


def main():
    # BZ2_dums_extractor.extract_bz2_dump('25')
    BZ2_dums_extractor.extract_all()
    Wiki_sql_extractor.extract_all()


if __name__ == '__main__':
    main()
