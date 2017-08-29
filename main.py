import extractors
import refiners
from update import update
from wiki_dump_generator import build_dump


def main():
    update.start_update()

if __name__ == '__main__':
    main()
