from html.entities import name2codepoint

from .CleanerConfig import *


def dropSpans(spans, text):
    """
    Drop from text the blocks identified in :param spans:, possibly nested.
    """
    spans.sort()
    res = ''
    offset = 0
    for s, e in spans:
        if offset <= s:         # handle nesting
            if offset < s:
                res += text[offset:s]
            offset = e
    res += text[offset:]
    return res


def dropNested(text, openDelim, closeDelim):
    """
    A matching function for nested expressions, e.g. namespaces and tables.
    """
    openRE = re.compile(openDelim, re.IGNORECASE)
    closeRE = re.compile(closeDelim, re.IGNORECASE)
    # partition text in separate blocks { } { }
    spans = []                  # pairs (s, e) for each partition
    nest = 0                    # nesting level
    start = openRE.search(text, 0)
    if not start:
        return text
    end = closeRE.search(text, start.end())
    next = start
    while end:
        next = openRE.search(text, next.end())
        if not next:            # termination
            while nest:         # close all pending
                nest -= 1
                end0 = closeRE.search(text, end.end())
                if end0:
                    end = end0
                else:
                    break
            spans.append((start.start(), end.end()))
            break
        while end.end() < next.start():
            # { } {
            if nest:
                nest -= 1
                # try closing more
                last = end.end()
                end = closeRE.search(text, end.end())
                if not end:     # unbalanced
                    if spans:
                        span = (spans[0][0], last)
                    else:
                        span = (start.start(), last)
                    spans = [span]
                    break
            else:
                spans.append((start.start(), end.end()))
                # advance start, find next close
                start = next
                end = closeRE.search(text, next.end())
                break           # { }
        if next != start:
            # { { }
            nest += 1
    # collect text outside partitions
    return dropSpans(spans, text)


def unescape(text):
    """
    Removes HTML or XML character references and entities from a text string.

    :param text The HTML (or XML) source text.
    :return The plain text, as a Unicode string, if necessary.
    """

    def fixup(m):
        text = m.group(0)
        code = m.group(1)
        try:
            if text[1] == "#":  # character reference
                if text[2] == "x":
                    return chr(int(code[1:], 16))
                else:
                    return chr(int(code))
            else:  # named entity
                return chr(name2codepoint[code])
        except:
            return text  # leave as is

    return re.sub("&#?(\w+);", fixup, text)


def transform1(text):
    """Transform text not containing <nowiki>"""
    # Drop transclusions (template, parser functions)
    text = dropNested(text, r'{{', r'}}')
    return text


def findBalanced(text, openDelim=['[['], closeDelim=[']]']):
    """
    Assuming that text contains a properly balanced expression using
    :param openDelim: as opening delimiters and
    :param closeDelim: as closing delimiters.
    :return: an iterator producing pairs (start, end) of start and end
    positions in text containing a balanced expression.
    """
    openPat = '|'.join([re.escape(x) for x in openDelim])
    # pattern for delimiters expected after each opening delimiter
    afterPat = {o: re.compile(openPat + '|' + c, re.DOTALL) for o, c in zip(openDelim, closeDelim)}
    stack = []
    start = 0
    cur = 0
    # end = len(text)
    startSet = False
    startPat = re.compile(openPat)
    nextPat = startPat
    while True:
        next = nextPat.search(text, cur)
        if not next:
            return
        if not startSet:
            start = next.start()
            startSet = True
        delim = next.group(0)
        if delim in openDelim:
            stack.append(delim)
            nextPat = afterPat[delim]
        else:
            opening = stack.pop()
            # assert opening == openDelim[closeDelim.index(next.group(0))]
            if stack:
                nextPat = afterPat[stack[-1]]
            else:
                yield start, next.end()
                nextPat = startPat
                start = next.end()
                startSet = False
        cur = next.end()


def makeInternalLink(title, label):
    colon = title.find(':')
    if colon > 0 and title[:colon] not in acceptedNamespaces:
        return ''
    if colon == 0:
        # drop also :File:
        colon2 = title.find(':', colon + 1)
        if colon2 > 1 and title[colon + 1:colon2] not in acceptedNamespaces:
            return ''
    return label


def replaceInternalLinks(text, specify_wikilinks):
    """
    Replaces internal links of the form:
    [[title |...|label]]trail

    with title concatenated with trail, when present, e.g. 's' for plural.

    See https://www.mediawiki.org/wiki/Help:Links#Internal_links
    """
    # call this after removal of external links, so we need not worry about
    # triple closing ]]].
    cur = 0
    res = ''
    for s, e in findBalanced(text):
        m = tailRE.match(text, e)
        if m:
            trail = m.group(0)
            end = m.end()
        else:
            trail = ''
            end = e
        inner = text[s + 2:e - 2]
        # find first |
        pipe = inner.find('|')
        if pipe < 0:
            title = inner
            label = title
        else:
            title = inner[:pipe].rstrip()
            # find last |
            curp = pipe + 1
            for s1, e1 in findBalanced(inner):
                last = inner.rfind('|', curp, s1)
                if last >= 0:
                    pipe = last  # advance
                curp = e1
            label = inner[pipe + 1:].strip()
        if specify_wikilinks:
            res += text[cur:s] + 'http://fa.wikipedia.org/wiki/' + title.replace(' ', '_') + ' ' + trail
        else:
            res += text[cur:s] + makeInternalLink(title, label) + trail
        cur = end
    return res + text[cur:]


def replaceExternalLinks(text):
    """
    https://www.mediawiki.org/wiki/Help:Links#External_links
    [URL anchor text]
    """
    s = ''
    cur = 0
    for m in ExtLinkBracketedRegex.finditer(text):
        s += text[cur:m.start()]
        cur = m.end()

        url = m.group(1)
        label = m.group(3)

        # # The characters '<' and '>' (which were escaped by
        # # removeHTMLtags()) should not be included in
        # # URLs, per RFC 2396.
        # m2 = re.search('&(lt|gt);', url)
        # if m2:
        #     link = url[m2.end():] + ' ' + link
        #     url = url[0:m2.end()]

        # If the link text is an image URL, replace it with an <img> tag
        # This happened by accident in the original parser, but some people used it extensively
        m = EXT_IMAGE_REGEX.match(label)
        if m:
            label = ''

        # Use the encoded URL
        # This means that users can paste URLs directly into the text
        # Funny characters like ö aren't valid in URLs anyway
        # This was changed in August 2004
        s += label

    return s + text[cur:]


def transform(wikitext):
    """
    Transforms wiki markup.
    @see https://www.mediawiki.org/wiki/Help:Formatting
    """
    # look for matching <nowiki>...</nowiki>
    res = ''
    cur = 0
    for m in nowiki.finditer(wikitext, cur):
        res += transform1(wikitext[cur:m.start()]) + wikitext[m.start():m.end()]
        cur = m.end()
    # leftover
    res += transform1(wikitext[cur:])
    return res


def wiki2text(text, specify_wikilinks):
    #
    # final part of internalParse().)
    #
    # $text = $this->doTableStuff( $text );
    # $text = preg_replace( '/(^|\n)-----*/', '\\1<hr />', $text );
    # $text = $this->doDoubleUnderscore( $text );
    # $text = $this->doHeadings( $text );
    # $text = $this->replaceInternalLinks( $text );
    # $text = $this->doAllQuotes( $text );
    # $text = $this->replaceExternalLinks( $text );
    # $text = str_replace( self::MARKER_PREFIX . 'NOPARSE', '', $text );
    # $text = $this->doMagicLinks( $text );
    # $text = $this->formatHeadings( $text, $origText, $isMain );

    # Drop tables
    # first drop residual templates, or else empty parameter |} might look like end of table.
    text = dropNested(text, r'{{', r'}}')
    text = dropNested(text, r'{\|', r'\|}')

    # Handle bold/italic/quote
    text = bold_italic.sub(r'\1', text)
    text = bold.sub(r'\1', text)
    text = italic_quote.sub(r'\1', text)
    text = italic.sub(r'\1', text)
    text = quote_quote.sub(r'\1', text)
    # residuals of unbalanced quotes
    text = text.replace("'''", '').replace("''", '"')

    # replace internal links
    text = replaceInternalLinks(text, specify_wikilinks)

    # replace external links
    text = replaceExternalLinks(text)

    # drop MagicWords behavioral switches
    text = magicWordsRE.sub('', text)

    # ############### Process HTML ###############

    # turn into HTML, except for the content of <syntaxhighlight>
    res = ''
    cur = 0
    for m in syntaxhighlight.finditer(text):
        res += unescape(text[cur:m.start()]) + m.group(1)
        cur = m.end()
    text = res + unescape(text[cur:])

    return text


def clean(wikitext, specify_wikilinks=True):
    # todo: check with our own entities for fake wiki links
    """
    Removes irrelevant parts from :param: text.
    """
    text = transform(wikitext)
    text = wiki2text(text, specify_wikilinks)
    # Drop discarded elements
    for tag in discardElements:
        text = dropNested(text, r'<\s*%s\b[^>/]*>' % tag, r'<\s*/\s*%s>' % tag)

    # Collect spans
    spans = []

    # Drop HTML comments
    for m in comment.finditer(text):
        spans.append((m.start(), m.end()))

    # Drop self-closing tags
    for pattern in selfClosing_tag_patterns:
        for m in pattern.finditer(text):
            spans.append((m.start(), m.end()))

    # Drop ignored tags
    for left, right in ignored_tag_patterns:
        for m in left.finditer(text):
            spans.append((m.start(), m.end()))
        for m in right.finditer(text):
            spans.append((m.start(), m.end()))

    # Bulk remove all spans
    text = dropSpans(spans, text)

    # Turn into text what is left (&amp;nbsp;) and <syntaxhighlight>
    text = unescape(text)

    # Expand placeholders
    for pattern, placeholder in placeholder_tag_patterns:
        index = 1
        for match in pattern.finditer(text):
            text = text.replace(match.group(), '%s_%d' % (placeholder, index))
            index += 1

    text = text.replace('<<', '«').replace('>>', '»')

    #############################################

    # Cleanup text
    text = text.replace('\t', ' ')
    text = spaces.sub(' ', text)
    text = dots.sub('...', text)
    text = re.sub(' (,:\.\)\]»)', r'\1', text)
    text = re.sub('(\[\(«) ', r'\1', text)
    text = re.sub(r'\n\W+?\n', '\n', text, flags=re.U)  # lines with only punctuations
    text = text.replace(',,', ',').replace(',.', '.')
    text = re.sub(r"\s\s+", '  ', text).strip()
    return text
