import wikitextparser as wtp
from pprint import pprint

text = open('Mosadegh.txt', 'r+').read()

wiki_text = wtp.parse(text)
# pprint(wiki_text.templates)
# pprint(wiki_text.comments)
# pprint(wiki_text.external_links)
# pprint(wiki_text.string)
# pprint(wiki_text.tables)
pprint(wiki_text.sections[0])
# pprint(wiki_text.wikilinks)


# for template in wiki_text.templates:
#     for t in template.templates:
#         print(t)
    # for arg in template.arguments:
    #     print(arg.name)
    #     print(arg.value)
    #     print('---------------------------')
