import sys

import six
from chameleon import PageTemplate
import random

BIGTABLE_ZPT = """\
<table xmlns="http://www.w3.org/1999/xhtml"
xmlns:tal="http://xml.zope.org/namespaces/tal">
<tr tal:repeat="row python: options['table']">
<td tal:repeat="c python: row.values()">
<span tal:define="d python: c + 1"
tal:attributes="class python: 'column-' + %s(d)"
tal:content="python: d" />
</td>
</tr>
</table>""" % six.text_type.__name__


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == 'long':
        num_of_rows = [1024, 2048]
        num_of_cols = [1024, 2048, 4096]
    elif mode == 'short':
        num_of_rows = [1024]
        num_of_cols = [1024]
    else:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    KEY = b'\xa1\xf6%\x8c\x87}_\xcd\x89dHE8\xbf\xc9,'
    for rows in num_of_rows:
        for cols in num_of_cols:
            tmpl = PageTemplate(BIGTABLE_ZPT)
            data = {}
            for i in range(cols):
                data[str(i)] = i
            table = [data for x in range(rows)]
            options = {'table': table}
            data = tmpl.render(options=options)
