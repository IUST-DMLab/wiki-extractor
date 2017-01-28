import re

acceptedNamespaces = ['w', 'wiktionary', 'wikt']

# Drop these elements from article text
discardElements = ['gallery', 'timeline', 'noinclude', 'pre', 'table', 'tr', 'td', 'th', 'caption', 'div', 'form',
                   'input', 'select', 'option', 'textarea', 'ul', 'li', 'ol', 'dl', 'dt', 'dd', 'menu', 'dir',
                   'ref', 'references', 'img', 'imagemap', 'source', 'small', 'sub', 'sup', 'indicator']

# Match HTML comments
comment = re.compile(r'<!--.*?-->', re.DOTALL)
# The buggy template {{Template:T}} has a comment terminating with just "->"

selfClosingTags = ('br', 'hr', 'nobr', 'ref', 'references', 'nowiki')

# These tags are dropped, keeping their content.
ignoredTags = ['abbr', 'b', 'big', 'blockquote', 'center', 'cite', 'em', 'font', 'h1', 'h2', 'h3', 'h4',
               'hiero', 'i', 'kbd', 'p', 'plaintext', 's', 'span', 'strike', 'strong', 'tt', 'u', 'var', 'a',
               'br', 'small', 'div', 'td', 'nowiki', 'ref', 'sup']

placeholder_tags = {'math': 'formula', 'code': 'codice'}

# Match selfClosing HTML tags
selfClosing_tag_patterns = [
    re.compile(r'<\s*%s\b[^>]*?/\s*>' % tag, re.DOTALL | re.IGNORECASE) for tag in selfClosingTags
    ]

# Match HTML placeholder tags
placeholder_tag_patterns = [
    (re.compile(r'<\s*%s(\s*| [^>]+?)>.*?<\s*/\s*%s\s*>' % (tag, tag), re.DOTALL | re.IGNORECASE),
     repl) for tag, repl in placeholder_tags.items()
    ]

# Matches bold/italic
bold_italic = re.compile(r"'''''(.*?)'''''")
bold = re.compile(r"'''(.*?)'''")
italic_quote = re.compile(r"''\"([^\"]*?)\"''")
italic = re.compile(r"''(.*?)''")
quote_quote = re.compile(r'""([^"]*?)""')

# Matches space
spaces = re.compile(r' {2,}')

# Matches dots
dots = re.compile(r'\.{4,}')

# Match <nowiki>...</nowiki>
nowiki = re.compile(r'<nowiki>.*?</nowiki>')

# match tail after wikilink
tailRE = re.compile('\w+')

syntaxhighlight = re.compile('&lt;syntaxhighlight .*?&gt;(.*?)&lt;/syntaxhighlight&gt;', re.DOTALL)

# ----------------------------------------------------------------------
# External links

# from: https://doc.wikimedia.org/mediawiki-core/master/php/DefaultSettings_8php_source.html

wgUrlProtocols = [
    'bitcoin:', 'ftp://', 'ftps://', 'geo:', 'git://', 'gopher://', 'http://',
    'https://', 'irc://', 'ircs://', 'magnet:', 'mailto:', 'mms://', 'news:',
    'nntp://', 'redis://', 'sftp://', 'sip:', 'sips:', 'sms:', 'ssh://',
    'svn://', 'tel:', 'telnet://', 'urn:', 'worldwind://', 'xmpp:', '//'
]

# from: https://doc.wikimedia.org/mediawiki-core/master/php/Parser_8php_source.html

# Constants needed for external link processing
# Everything except bracket, space, or control characters
# \p{Zs} is unicode 'separator, space' category. It covers the space 0x20
# as well as U+3000 is IDEOGRAPHIC SPACE for bug 19052
EXT_LINK_URL_CLASS = r'[^][<>"\x00-\x20\x7F\s]'
ANCHOR_CLASS = r'[^][\x00-\x08\x0a-\x1F]'
ExtLinkBracketedRegex = re.compile(
    '\[(((?i)' + '|'.join(wgUrlProtocols) + ')' + EXT_LINK_URL_CLASS + r'+)' +
    r'\s*((?:' + ANCHOR_CLASS + r'|\[\[' + ANCHOR_CLASS + r'+\]\])' + r'*?)\]',
    re.S | re.U)
# A simpler alternative:
# ExtLinkBracketedRegex = re.compile(r'\[(.*?)\](?!])')

EXT_IMAGE_REGEX = re.compile(
    r"""^(http://|https://)([^][<>"\x00-\x20\x7F\s]+)
    /([A-Za-z0-9_.,~%\-+&;#*?!=()@\x80-\xFF]+)\.((?i)gif|png|jpg|jpeg)$""",
    re.X | re.S | re.U)


switches = ['__NOTOC__', '__FORCETOC__', '__TOC__', '__TOC__', '__NEWSECTIONLINK__', '__NONEWSECTIONLINK__',
            '__NOGALLERY__', '__HIDDENCAT__', '__NOCONTENTCONVERT__', '__NOCC__', '__NOTITLECONVERT__', '__NOTC__',
            '__START__', '__END__', '__INDEX__', '__NOINDEX__', '__STATICREDIRECT__', '__DISAMBIG__']

magicWordsRE = re.compile('|'.join(switches))

# Match ignored tags
ignored_tag_patterns = []


for tag in ignoredTags:
    left = re.compile(r'<%s\b.*?>' % tag, re.IGNORECASE | re.DOTALL)  # both <ref> and <reference>
    right = re.compile(r'</\s*%s>' % tag, re.IGNORECASE)
    ignored_tag_patterns.append((left, right))
