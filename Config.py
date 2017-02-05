from os.path import join, dirname, realpath

extracted_pages_per_file = 100000
logging_interval = 10000

current_dir = dirname(realpath(__file__))
resources_dir = join(current_dir, 'resources')
wikipedia_dumps_dir = join(resources_dir, 'dumps')
extracted_dir = join(resources_dir, 'extracted')
extracted_pages_articles_dir = join(extracted_dir, 'pages-articles')
extracted_pages_meta_current_dir = join(extracted_dir, 'pages-meta-current')
extracted_pages_with_infobox_dir = join(extracted_dir, 'with_infobox')
extracted_pages_without_infobox_dir = join(extracted_dir, 'without_infobox')
extracted_pages_path_dir = join(extracted_dir, 'pages_path')
extracted_ids_dir = join(extracted_dir, 'ids')
extracted_revision_ids_dir = join(extracted_dir, 'revision_ids')
extracted_infoboxes_dir = join(extracted_dir, 'infoboxes')
extracted_abstracts_dir = join(extracted_dir, 'abstracts')
extracted_disambiguation_dir = join(extracted_dir, 'disambiguation')

json_result_dir = join(resources_dir, 'json-result')

fawiki_latest_pages_articles_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pages-articles.xml.bz2')
fawiki_latest_pages_meta_current_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pages-meta-current.xml.bz2')

fawiki_latest_category_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-categorylinks.sql.gz')
fawiki_latest_external_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-externallinks.sql.gz')
fawiki_latest_lang_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-langlinks.sql.gz')
fawiki_latest_page_dump = join(wikipedia_dumps_dir, 'fawiki-latest-page.sql.gz')
fawiki_latest_page_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pagelinks.sql.gz')
fawiki_latest_redirect_dump = join(wikipedia_dumps_dir, 'fawiki-latest-redirect.sql.gz')

infobox_flags_en = ['infobox', 'taxobox', 'drugbox', 'geobox', 'ionbox', 'planetbox', 'chembox',
                    'starbox', 'drugclassbox', 'reactionbox', 'speciesbox', 'comiccharacterbox']

infobox_flags_fa = ['جعبه اطلاعات', 'جعبه']

redirect_flags = ['#تغییر_مسیر', '#تغییرمسیر', '#REDIRECT', '#redirect']

disambigution_flags = ['رفع‌ابهام‌', 'رفع ابهام', 'ابهام‌زدایی', 'ابهام زدایی']
