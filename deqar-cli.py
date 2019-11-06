#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import deqar
from tabulate import tabulate
import click
import os

"""
CLI functions
"""

def coalesce(d, key):
    return d[key] if key in d else '-'

def myjoin(l):
    return ', '.join(l) if len(l) > 1 else ( l[0] if l[0] else '-' )

def show_results(results):
    width = os.get_terminal_size().columns
    """
        # + ID + ETER + space   : 35
        name min                : 15
        country/city min        : 10
                                  --
                                  70
    """
    if width >= 70:
        name_width = 15 + (width - 70) // 2
        city_width = 10 + (width - 70) // 4
        result_table = {
            click.style('ID', fg='green', bold=True):       [ 'DEQARINST' + i['id'] for i in results ],
            click.style('ETER ID', fg='green', bold=True):  [ coalesce(i,'eter_id') for i in results ],
            click.style('Name', fg='green', bold=True):     [ i['name_primary'][:name_width] for i in results ],
            click.style('City', fg='green', bold=True):     [ myjoin([ coalesce(j, 'city') for j in i['place']])[:city_width] for i in results ],
            click.style('Country', fg='green', bold=True):  [ myjoin([ coalesce(j, 'country') for j in i['place']])[:city_width] for i in results ]
        }
        print(tabulate(result_table, headers='keys', showindex=True))
    else:
        width = width - 4
        n = 0
        for i in results:
            print("\n{}: DEQARINST{} / ETER {}\n{}, {} ({})".format(
                n,
                i['id'],
                coalesce(i,'eter_id'),
                i['name_primary'][:width],
                myjoin([ coalesce(j, 'city') for j in i['place']])[:width],
                myjoin([ coalesce(j, 'country') for j in i['place']])[:width]))
            n += 1


def paginate_heis(query):
    offset, limit, last_offset = 0, 20, -1
    try:
        while True:
            if offset != last_offset:
                result = api.get_institutions(query=query, limit=limit, offset=offset)
                last_offset = offset
            if result and result[u'count'] > 0:
                count = int(result[u'count'])
                click.secho(u' => {} to {} of {} results\n'.format(offset+1,min(offset+limit,count),count), bold=True)
                show_results(result['results'])
                navigate = input(u'\n  View HEI: ' + click.style('0-9', bold=True) + '+ - Navigate: '
                                + click.style('F', bold=True) + 'irst '
                                + click.style('P', bold=True) + 'revious '
                                + click.style('N', bold=True) + 'ext '
                                + click.style('L', bold=True) + 'ast '
                                + click.style('Q', bold=True) + 'uit\n  => ');
                if navigate.isdigit():
                    if 0 <= int(navigate) < min(limit,count):
                        show_hei(int(result[u'results'][int(navigate)][u'id']))
                    else:
                        print('Invalid choice.')
                elif len(navigate) < 1:
                    pass
                elif navigate[0] in set('FfPpNnLlQq'):
                    choice = navigate[0].upper()
                    if choice == 'F':
                        offset = 0
                    elif choice == 'P':
                        offset = max(0,offset - 20)
                    elif choice == 'N':
                        offset = offset + 20 if offset < count - 20 else offset
                    elif choice == 'L':
                        offset = max(0, count - 20)
                    elif choice == 'Q':
                        break;
                    else:
                        print('Invalid choice.')
            else:
                print(" - but no results, sorry.")
                break
    except (KeyboardInterrupt, EOFError):   # we catch those to return instead of to quit the whole programme
        pass

def show_hei(id):
    result = api.get_institution(id)
    if result:
        print("""
{names[0][name_official]}
{names[0][name_english]}

ID              DEQARINST{id}
ETER ID         {eter}
Website         {website_link}
Country         {countries[0][country][name_english]}
""".format(**result))
    else:
        print("HEI not found")


"""
__main__ : keep looping and ask for query string
"""

print("DEQAR Search Command-line interface (CLI) v0.1");
print("----------------------------------------------");

api = deqar.EqarApi("https://backend.deqar.eu/");

try:
    while True:
        query = input("\nQuery: ")
        if (len(query) > 0):
            if (query[0].upper() == 'Q'):
                raise EOFError
            paginate_heis(query)

except (KeyboardInterrupt, EOFError):
    print("\nGoodbye.");


