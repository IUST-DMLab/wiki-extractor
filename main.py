import extractors
import refiners
from update import update


def main():
    update.start_update(force_update=True)

if __name__ == '__main__':
    main()
