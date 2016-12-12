import bz2
from bs4 import BeautifulSoup


def get_pages_xml(dump_filename):
    dump_file = bz2.open(dump_filename, mode='rt', encoding='utf8')

    for l in dump_file:
        page = list()
        if l.strip() == '<page>':
            page.append(l)
            while l.strip() != '</page>':
                l = next(dump_file)
                page.append(l)
        if page:
            yield '\n'.join(page)


def get_wikipedia_pages(dump_filename='resources/fawiki-latest-pages-meta-current.xml.bz2'):
    xml_pages = get_pages_xml(dump_filename)
    for xml_page in xml_pages:
        soup = BeautifulSoup(xml_page, "xml")
        page = soup.find('page')
        yield page


def main():
    wikipedia_pages = get_wikipedia_pages()
    for page in wikipedia_pages:
        print(page.title.text)
        wikitext = page.revision.find('text').text
        print(wikitext)


if __name__ == '__main__':
    main()