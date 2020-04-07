#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import getpass
import datetime
import psycopg2
import psycopg2.extras
import urllib.parse

def make_redirect(conn):

    """
    Generates redirect statements for short URLs leading to long URLs including pre-filled form values.
    """

    cur = conn.cursor()

    cur.execute("""
        select
            agency_acronym,
            agency_name,
            agency_id,
            case when sum(reports) > 0 then true else false end as in_deqar,
            case when sum(reports) > 0 then json_agg(json_build_object(
                'iso_3166_alpha3', iso_3166_alpha3,
                'country', name_english,
                'type', activity_type,
                'activity', activity,
                'reports', reports
            )) else json_build_array() end as deqar_info
        from (
            select
                deqar_agencies.acronym_primary as agency_acronym,
                deqar_agencies.name_primary as agency_name,
                deqar_agencies.id as agency_id,
                deqar_countries.iso_3166_alpha3,
                deqar_countries.name_english,
                deqar_agency_activity_types.type as activity_type,
                deqar_agency_esg_activities.activity,
                count(distinct deqar_reports.id) as reports
            from deqar_agencies
            left join deqar_reports on (deqar_agencies.id = deqar_reports.agency_id and ( valid_from between %s and %s ) )
            left join deqar_agency_esg_activities on deqar_agency_esg_activities.id = deqar_reports.agency_esg_activity_id
            left join deqar_agency_activity_types on deqar_agency_activity_types.id = deqar_agency_esg_activities.activity_type_id
            left join deqar_reports_institutions on deqar_reports_institutions.report_id = deqar_reports.id
            left join deqar_institution_countries on deqar_institution_countries.institution_id = deqar_reports_institutions.institution_id
            left join deqar_countries on deqar_countries.id = deqar_institution_countries.country_id
            where is_registered
            group by
                deqar_agencies.id,
                deqar_agencies.name_primary,
                acronym_primary,
                iso_3166_alpha3,
                deqar_countries.name_english,
                deqar_agency_activity_types.type,
                deqar_agency_esg_activities.activity
            order by
                acronym_primary,
                iso_3166_alpha3,
                deqar_agency_activity_types.type,
                deqar_agency_esg_activities.activity
        ) as report_stats
        group by
            agency_acronym,
            agency_name,
            agency_id
    """, (datetime.date(int(args.YEAR), 1, 1), datetime.date(int(args.YEAR), 12, 31)))

    for row in cur:
        if row['in_deqar']:
            deqar_info = "\n".join( "{0[iso_3166_alpha3]} {0[country]:15.15} - {0[activity]:41.41} ({0[type]:^15.15}): {0[reports]:4}".format(i) for i in row['deqar_info'])
        else:
            deqar_info = ""

        print("Redirect /annual-update/{} https://fs22.formsite.com/EQAR_forms/annual_update/fill?{}".format(row['agency_id'], urllib.parse.urlencode(dict(
            id4=row['agency_name'],
            id174=row['agency_acronym'],
            id179="https://data.deqar.eu/agency/{}".format(row['agency_id']),
            id182=int(row['in_deqar']),
            id165=deqar_info
        ))))

def make_sql(conn):

    """
    Generates SQL statements for import of data to EQAR Contact DB.
    """

    cur = conn.cursor()

    cur.execute("""
            select
                deqar_agencies.acronym_primary as agency_acronym,
                deqar_agencies.name_primary as agency_name,
                deqar_agencies.id as agency_id,
                deqar_countries.iso_3166_alpha3,
                deqar_countries.name_english,
                deqar_agency_focus_countries.country_is_crossborder,
                deqar_agency_activity_types.type as activity_type,
                count(distinct deqar_reports.id) as reports
            from deqar_reports
            left join deqar_agencies on deqar_agencies.id = deqar_reports.agency_id
            left join deqar_agency_esg_activities on deqar_agency_esg_activities.id = deqar_reports.agency_esg_activity_id
            left join deqar_agency_activity_types on deqar_agency_activity_types.id = deqar_agency_esg_activities.activity_type_id
            left join deqar_reports_institutions on deqar_reports_institutions.report_id = deqar_reports.id
            left join deqar_institution_countries on deqar_institution_countries.institution_id = deqar_reports_institutions.institution_id
            left join deqar_countries on deqar_countries.id = deqar_institution_countries.country_id
            left join deqar_agency_focus_countries on deqar_agency_focus_countries.country_id = deqar_institution_countries.country_id
                                                  and deqar_agency_focus_countries.agency_id = deqar_reports.agency_id
            where deqar_reports.valid_from between %s and %s
            group by
                deqar_agencies.id,
                deqar_agencies.name_primary,
                acronym_primary,
                iso_3166_alpha3,
                deqar_countries.name_english,
                deqar_agency_focus_countries.country_is_crossborder,
                deqar_agency_activity_types.type
            order by
                acronym_primary,
                iso_3166_alpha3,
                deqar_agency_activity_types.type
    """, (datetime.date(int(args.YEAR), 1, 1), datetime.date(int(args.YEAR), 12, 31)))

    # wrap in one transaction
    print("BEGIN;")

    for row in cur:
        row['country_is_crossborder'] = int(row['country_is_crossborder'])
        print("INSERT INTO agencyUpdate ( rid, country, year, type, amount, crossBorder, source ) VALUES ( ( SELECT rid FROM registeredAgency WHERE deqarId = '{0[agency_id]}' ), '{0[iso_3166_alpha3]}', '{1}', '{0[activity_type]}', '{0[reports]}', '{0[country_is_crossborder]}', 'DEQAR' ); -- agency: {0[agency_acronym]}".format(row, args.YEAR))

    print("COMMIT;")

# arguments are mainly the DB connection, and the output mode
parser = argparse.ArgumentParser(description="Generate information for annual agency updates from DEQAR database.")
parser.add_argument("YEAR", help="Year for which data should be fetched")
parser.add_argument("-d", "--dbname", help="Database (default: $DEQAR_DB, or \'deqar\')")
parser.add_argument("-H", "--host", help="Database host (default: $DEQAR_HOST or localhost)")
parser.add_argument("-u", "--user", help="Database user (default: $DEQAR_USER or current user)")
parser.add_argument("-p", "--password", help="Database password (default: $DEQAR_PASSWORD or prompt)")
parser.add_argument("-m", "--mode", help="What to generate: redirect for pre-filled form, or SQL for loading to Contact DB", choices=['redirect', 'sql'], default='redirect')

# set default values
# - database name
if 'DEQAR_DB' in os.environ and os.environ['DEQAR_DB']:
    parser.set_defaults(dbname=os.environ['DEQAR_DB'])
else:
    parser.set_defaults(dbname='deqar')
# - database user
if 'DEQAR_USER' in os.environ and os.environ['DEQAR_USER']:
    parser.set_defaults(user=os.environ['DEQAR_USER'])
else:
    parser.set_defaults(user=getpass.getuser())
# - database host
#   (default might be None, which means local server)
if 'DEQAR_HOST' in os.environ and os.environ['DEQAR_HOST']:
    parser.set_defaults(host=os.environ['DEQAR_HOST'])
# - database password
#   (default might be None, which means prompt for password)
if 'DEQAR_PASSWORD' in os.environ and os.environ['DEQAR_PASSWORD']:
    parser.set_defaults(password=os.environ['DEQAR_PASSWORD'])

args = parser.parse_args()

dsn = dict( dbname=args.dbname,
            host=args.host,
            user=args.user,
            password=args.password )

# we prompt for password if none was set
if dsn['password'] is None:
    dsn['password'] = getpass.getpass()

# connect to PostgreSQL
conn = psycopg2.connect(**dsn, cursor_factory=psycopg2.extras.DictCursor)

try:

    if args.mode == 'redirect':
        make_redirect(conn)
    elif args.mode == 'sql':
        make_sql(conn)

finally:
    conn.close()

