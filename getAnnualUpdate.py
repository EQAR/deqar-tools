#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import getpass
import datetime
import psycopg2
import psycopg2.extras
import urllib.parse
from jinja2 import Environment, FileSystemLoader, select_autoescape

def make_redirect(conn):

    """
    Generates simple HTML forms that redirect to a pre-filled form.

    (uses HTTP POST instead of redirect URLs, as they can grow too long)
    """

    cur = conn.cursor()

    cur.execute("""
        select
            agency_acronym,
            agency_name,
            agency_id,
            email,
            username,
            case when max(reports_total) > 0 then true else false end as in_deqar,
            max(reports_total) as reports_total,
            sum(reports_year) as reports_year,
            json_agg(json_build_object(
                'iso_3166_alpha3', coalesce(iso_3166_alpha3,'   '),
                'country', coalesce(name_english, ''),
                'type', activity_type,
                'activity', activity,
                'reports', reports_year
            )) as deqar_info
        from (
            select
                deqar_agencies.id as agency_id,
                deqar_agencies.acronym_primary as agency_acronym,
                deqar_agencies.name_primary as agency_name,
                deqar_agencies.reports_total,
                deqar_agencies.email,
                deqar_agencies.username,
                deqar_countries.iso_3166_alpha3,
                deqar_countries.name_english,
                deqar_agency_activity_types.type as activity_type,
                deqar_agency_esg_activities.activity,
                count(distinct deqar_reports.id) as reports_year
            from (
                select
                    deqar_agencies.id,
                    deqar_agencies.acronym_primary,
                    deqar_agencies.name_primary,
                    count(distinct deqar_reports.id) as reports_total,
                    auth_user.email,
                    auth_user.username
                from deqar_agencies
                left join deqar_reports on deqar_reports.agency_id = deqar_agencies.id
                left join deqar_agency_submitting_agencies on deqar_agency_submitting_agencies.agency_id = deqar_agencies.id
                left join accounts_deqarprofile on accounts_deqarprofile.submitting_agency_id = deqar_agency_submitting_agencies.id
                left join auth_user on auth_user.id = accounts_deqarprofile.user_id
                where is_registered
                group by
                    deqar_agencies.id,
                    name_primary,
                    acronym_primary,
                    email,
                    username
            ) as deqar_agencies
            left join deqar_agency_esg_activities on deqar_agency_esg_activities.agency_id = deqar_agencies.id
            left join deqar_agency_activity_types on deqar_agency_activity_types.id = deqar_agency_esg_activities.activity_type_id
            left join deqar_reports on ( deqar_reports.agency_esg_activity_id = deqar_agency_esg_activities.id and ( deqar_reports.valid_from between %s and %s ) )
            left join deqar_reports_institutions on deqar_reports_institutions.report_id = deqar_reports.id
            left join deqar_institution_countries on deqar_institution_countries.institution_id = deqar_reports_institutions.institution_id
            left join deqar_countries on deqar_countries.id = deqar_institution_countries.country_id
            group by
                deqar_agencies.id,
                deqar_agencies.acronym_primary,
                deqar_agencies.name_primary,
                deqar_agencies.reports_total,
                email,
                username,
                iso_3166_alpha3,
                deqar_countries.name_english,
                deqar_agency_activity_types.type,
                deqar_agency_esg_activities.activity
            order by
                acronym_primary,
                deqar_agency_activity_types.type,
                deqar_agency_esg_activities.activity,
                iso_3166_alpha3
        ) as report_stats
        group by
            agency_acronym,
            agency_name,
            agency_id,
            email,
            username
    """, (datetime.date(int(args.YEAR), 1, 1), datetime.date(int(args.YEAR), 12, 31)))

    # load Jinja2 template
    env = Environment(
        loader=FileSystemLoader(".")
    )
    tmpl = env.get_template("annual-update.tmpl")

    for row in cur:
        # for agencies in DEQAR, simple stats of last year's reports will be shown in a textarea
        if row['in_deqar']:
            deqar_info = "\n".join( "{0[activity]:41.41} ({0[type]:^15.15}) - {0[iso_3166_alpha3]} {0[country]:15.15} : {0[reports]:4}".format(i) for i in row['deqar_info'])
        else:
            deqar_info = ""

        # basic dict with agency's info
        parameters = dict(
            id247=row['agency_id'],
            id4=row['agency_name'],
            id174=row['agency_acronym'],
            id179="https://data.deqar.eu/agency/{}".format(row['agency_id']),
            id182=int(row['in_deqar']),
            id165=deqar_info,
            id222=row['username'],
            id223=row['email']
        )

        # for agencies not in DEQAR, pre-fill ESG activity names for matrix input
        if not row['in_deqar']:
            for i in range(16): # currently, hard limit of 16 activities
                if i < len(row['deqar_info']):
                    parameters[f"id{230+i}"] = row['deqar_info'][i]['activity']
                else:
                    parameters[f"id225-{i+1}-1"] = '-'

        # output form
        if args.output:
            thisfile = os.path.join(args.output, f"{row['agency_id']}.html")
            print(f"- Saving form for {row['agency_acronym']} to {thisfile}")
        else:
            thisfile = sys.stdout

        tmpl.stream(
            agency_name=row['agency_name'],
            agency_acronym=row['agency_acronym'],
            agency_url="https://data.deqar.eu/agency/{}".format(row['agency_id']),
            form=parameters
        ).dump(thisfile)

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
        row['country_is_crossborder'] = 1 if row['country_is_crossborder'] else 0
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
parser.add_argument("-o", "--output", help="Directory where to create HTML forms with redirects (default: dump to stdout)")

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

