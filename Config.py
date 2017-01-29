from os.path import join, dirname, realpath

extracted_pages_per_file = 100000
logging_interval = 10000

MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASS = ''
MYSQL_DB = 'wiki'

current_dir = dirname(realpath(__file__))
resources_dir = join(current_dir, 'resources')
wikipedia_dumps_dir = join(resources_dir, 'dumps')
extracted_dir = join(resources_dir, 'extracted')
extracted_pages_articles_dir = join(extracted_dir, 'pages-articles')
extracted_pages_meta_current_dir = join(extracted_dir, 'pages-meta-current')
extracted_ids_dir = join(extracted_dir, 'ids')
extracted_revision_ids_dir = join(extracted_dir, 'revision_ids')
extracted_infoboxes_dir = join(extracted_dir, 'infoboxes')
extracted_abstracts_dir = join(extracted_dir, 'abstracts')

fawiki_latest_pages_articles_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pages-articles.xml.bz2')
fawiki_latest_pages_meta_current_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pages-meta-current.xml.bz2')

infobox_flags = ['Infobox', 'infobox', 'Taxobox', 'taxobox', 'Drugbox', 'drugbox', 'Geobox', 'geobox',
                 'Ionbox', 'ionbox', 'Planetbox', 'Planetbox', 'Chembox', 'chembox', 'Starbox', 'starbox',
                 'Drugclassbox', 'drugclassbox', 'Reactionbox', 'reactionbox', 'Speciesbox', 'speciesbox',
                 'Comiccharacterbox', 'comiccharacterbox', 'جعبه اطلاعات', 'جعبه']

redirect_flags = ['#تغییر_مسیر', '#تغییرمسیر', '#REDIRECT', '#redirect']

fawiki_latest_category_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-categorylinks.sql')
fawiki_latest_external_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-externallinks.sql')
fawiki_latest_page_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pagelinks.sql')
fawiki_latest_redirect_dump = join(wikipedia_dumps_dir, 'fawiki-latest-redirect.sql')

json_result_dir = join(resources_dir, 'json-result')
