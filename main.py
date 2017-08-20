import extractors
import refiners
from update import update
from wiki_dump_generator import build_dump


def main():
    # update.start_update()
    # extractors.extract_fawiki_bz2_dump_information()
    # refiners.build_infobox_tuples()
    build_dump()

if __name__ == '__main__':
    main()
