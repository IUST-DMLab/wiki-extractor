from os.path import join, dirname, realpath

extracted_pages_per_file = 100000
logging_interval = 10000

current_dir = dirname(realpath(__file__))
resources_dir = join(current_dir, 'resources')
wikipedia_dumps_dir = join(resources_dir, 'dumps')
extracted_dir = join(resources_dir, 'extracted')
extracted_pages_dir = join(extracted_dir, 'pages')
extracted_ids_dir = join(extracted_dir, 'ids')
extracted_infoboxes_dir = join(extracted_dir, 'infoboxes')

fawiki_latest_pages_articles_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pages-articles.xml.bz2')
fawiki_latest_pages_meta_current_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pages-meta-current.xml.bz2')
