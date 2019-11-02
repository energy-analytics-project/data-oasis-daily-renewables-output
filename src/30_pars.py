#! /usr/bin/env python3
# edl : common library for the energy-dashboard tool-chain
# Copyright (C) 2019  Todd Greenwood-Geer (Enviro Software Solutions, LLC)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


# -----------------------------------------------------------------------------
# 30_pars.py : parse resources from structured text file into SQL for later insertion
# -----------------------------------------------------------------------------

from datetime import datetime
from dateutil import parser
from edl.resources import log
from edl.resources import state
import json
import logging
import os
import sys


# 20191030_DailyRenewablesWatch.txt
EXAMPLE = """
10/30/19			Hourly Breakdown of Renewable Resources (MW)
	Hour		GEOTHERMAL	BIOMASS		BIOGAS		SMALL HYDRO	WIND TOTAL	SOLAR PV	SOLAR THERMAL
	1		264		261		221		166		1818		0		0
	2		264		261		223		166		2018		0		0
	3		264		262		222		165		1976		0		0
	4		264		262		221		163		1601		0		0
	5		264		262		221		161		1375		0		0
	6		264		265		220		161		1313		0		0
	7		264		266		217		179		1398		0		0
	8		264		265		210		184		1267		606		0
	9		264		267		212		170		1182		3999		93
	10		264		258		215		154		1131		7296		344
	11		264		264		219		135		1148		8270		428
	12		264		250		220		138		1214		8696		434
	13		264		250		224		143		1297		7194		426
	14		264		274		229		164		1196		7125		422
	15		263		290		230		142		1105		8204		416
	16		263		292		231		157		1040		8072		385
	17		264		302		231		172		936		5410		285
	18		267		315		232		199		778		1083		38
	19		268		314		236		205		657		0		0
	20		268		314		218		202		586		0		0
	21		268		285		211		192		538		0		0
	22		268		284		215		193		485		0		0
	23		268		276		227		194		486		0		0
	24		267		270		233		174		427		0		0


			Hourly Breakdown of Total Production by Resource Type (MW)
	Hour		RENEWABLES	NUCLEAR		THERMAL		IMPORTS		HYDRO
	1		2730		1129		9802		5064		2074
	2		2932		1127		9280		4595		2074
	3		2625		1125		9507		4375		2074
	4		2511		1126		9612		4337		2073
	5		2283		1127		10124		4554		2038
	6		2223		1128		11619		4210		2038
	7		2324		1129		13873		4221		2038
	8		2796		1128		14539		4307		2038
	9		6187		1128		11731		3441		2038
	10		9662		1129		8256		2340		2038
	11		10728		1130		6825		2299		2038
	12		11216		1130		6400		1837		2038
	13		9798		1130		6327		2767		2107
	14		9674		1130		7000		2488		2110
	15		10650		1129		7056		1698		2102
	16		10440		1127		7699		1944		2110
	17		7600		1127		9944		3563		2110
	18		2912		1122		13944		5439		2080
	19		1680		1121		15993		6242		2094
	20		1588		1120		16004		6400		2094
	21		1494		1121		15196		6574		2079
	22		1445		1121		14578		6137		2076
	23		1451		1122		13373		5592		2070
	24		1371		1121		12330		5056		2068
"""


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def config():
    """
    config = {
            "source_dir"    : location of the source files
            "working_dir"   : location of the database
            "state_file"    : fqpath to file that lists the inserted source files
            }
    """
    cwd                     = os.path.abspath(os.path.curdir)
    config = {
            "source_dir"    : os.path.join(cwd, "txt"),
            "working_dir"   : os.path.join(cwd, "sql"),
            "state_file"    : os.path.join(cwd, "sql", "state.txt")
            }
    return config

# -----------------------------------------------------------------------------
# Text File Parser
# -----------------------------------------------------------------------------
def parse_text_files(logger, resource_name, new_files, txt_dir, sql_dir):
    for f in new_files:
        try:
            yield parse_text_file(logger, resource_name, txt_dir, sql_dir, f)
        except Exception as e:
            log.error(logger, {
                "name"      : __name__,
                "method"    : "parse_text_files",
                "src"       : "30_pars.py",
                "resource"  : resource_name,
                "input"     : os.path.join(txt_dir, f),
                "sql_dir"   : sql_dir,
                "exception" : str(e),
                })

def parse_text_file(logger, resource_name, txt_dir, sql_dir, f):
    input_file = os.path.join(txt_dir, f)
    (dict_renewable, dict_total) = read_file_name(input_file)
    renewable_sql   = gen_renewable_sql(dict_renewable)
    total_sql       = gen_total_sql(dict_total)
    (f_name, f_ext) = os.path.splitext(f)
    output_file = os.path.join(sql_dir, "%s.sql" % f_name)
    with open(output_file, 'w') as sqlfile:
        [sqlfile.write("%s\n" % line) for line in renewable_sql]
        [sqlfile.write("%s\n" % line) for line in total_sql]
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "parse_text_file",
        "src"       : "30_pars.py",
        "input"     : input_file,
        "output"    : output_file,
        })
    return f

# -----------------------------------------------------------------------------
# Text File Parser Helpers
# -----------------------------------------------------------------------------

def read_file_name(name):
    with open(name, 'r') as f:
        return read_file(f)

def read_file(fh):
    return read_data(fh.read())

def chunk(s):
    return s.split("Hourly")

def extract_date(s):
    s = s.lstrip().rstrip()
    d = parser.parse(s) 
    return d

def extract_table(date, s):
    # first line is header
    # second line is column names
    # remaining 24 lines are data
    lines       = s.split('\n')
    header      = lines[0].lstrip().rstrip()
    columns     = [s.replace(' ', '_') for s in list(filter(lambda x: len(x) > 0, lines[1].split('\t')))]
    data        = [x.split() for x in lines[2:]][:24]
    datamap     = {}
    for row in data:
        try:
            try:
                idx = int(row[0])
            except:
                idx = int(rount(float(row[0])))
            datamap[idx] = row
        except Exception as e:
            log.error(logger, {
                "name"      : __name__,
                "method"    : "extract_table",
                "src"       : "30_pars.py",
                "row"       : row,
                "raw_table" : s,
                "exception" : str(e),
                })
    return {
            'date'      : date,
            'header'    : header,
            'columns'   : columns,
            'data'      : datamap, 
            }

def read_data(s):
    (s_date, s_renewable, s_total) = chunk(s)
    date        = extract_date(s_date)
    renewable   = extract_table(date, s_renewable)
    total       = extract_table(date, s_total)
    return (renewable, total)


def gen_renewable_sql(t):
    #Hour		GEOTHERMAL	BIOMASS		BIOGAS		SMALL HYDRO	WIND TOTAL	SOLAR PV	SOLAR THERMAL						
    renewable_ddl   = 'CREATE TABLE IF NOT EXISTS renewable (id PRIMARY KEY ASC, date TEXT, hour INT, geothermal INT, biomass INT, biogas INT, small_hydro INT, wind_total INT, solar_pv INT, solar_thermal INT, solar INT, UNIQUE(date, hour));'
    res             = [renewable_ddl]
    for idx in range(1,26):
        try:
            if idx in t['data']:
                row         = t['data'][idx]
                date        = t['date']
                hour        = int(row[0])
                geothermal  = int_or_none(row[1])
                biomass     = int_or_none(row[2])
                biogas      = int_or_none(row[3])
                small_hydro = int_or_none(row[4])
                wind_total  = int_or_none(row[5])
                if len(row) == 8:
                    solar_pv    = int_or_none(row[6])
                    solar_thermal = int_or_none(row[7])
                    sql         = f'INSERT INTO renewable (date, hour, geothermal, biomass, biogas, small_hydro, wind_total, solar_pv, solar_thermal) VALUES ("{date}", {hour}, {geothermal}, {biomass}, {biogas}, {small_hydro}, {wind_total}, {solar_pv}, {solar_thermal});'
                else:
                    solar       = int_or_none(row[6])
                    sql         = f'INSERT INTO renewable (date, hour, geothermal, biomass, biogas, small_hydro, wind_total, solar) VALUES ("{date}", {hour}, {geothermal}, {biomass}, {biogas}, {small_hydro}, {wind_total}, {solar});'
                res.append(sql)
        except Exception as e:
            log.error(logger, {
                "name"      : __name__,
                "method"    : "gen_renewable_sql",
                "src"       : "30_pars.py",
                "date"      : t['date'].strftime('%Y%m%d'),
                "idx"       : idx,
                "exception" : str(e),
                })
    return res

def gen_total_sql(t):
    #Hour		RENEWABLES	NUCLEAR		THERMAL		IMPORTS		HYDRO							
    total_ddl       = 'CREATE TABLE IF NOT EXISTS total (id PRIMARY KEY ASC, date TEXT, hour INT, renewables INT, nuclear INT, thermal INT, imports INT, hydro INT, UNIQUE(date, hour));'
    res             = [total_ddl]
    for idx in range(1,26):
        try:
            if idx in t['data']:
                row         = t['data'][idx]
                date        = t['date']
                hour        = int(row[0])
                renewables  = int_or_none(row[1])
                nuclear     = int_or_none(row[2])
                thermal     = int_or_none(row[3])
                imports     = int_or_none(row[4])
                hydro       = int_or_none(row[5])
                sql         = f'INSERT INTO total (date, hour, renewables, nuclear, thermal, imports, hydro) VALUES ("{date}", {hour}, {renewables}, {nuclear}, {thermal}, {imports}, {hydro});'
                res.append(sql)
        except Exception as e:
            log.error(logger, {
                "name"      : __name__,
                "method"    : "gen_total_sql",
                "src"       : "30_pars.py",
                "date"      : t['date'].strftime('%Y%m%d'),
                "idx"       : idx,
                "exception" : str(e),
                })
    return res


def int_or_none(v):
    try:
        return int(v)
    except:
        return None

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(logger, manifest, config):
    resource_name   = manifest['name']
    resource_url    = manifest['url']
    txt_dir         = config['source_dir']
    sql_dir         = config['working_dir']
    state_file      = config['state_file']
    new_files = state.new_files(resource_name, state_file, txt_dir, 'DailyRenewablesWatch.txt')
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "run",
        "resource"  : resource_name,
        "url"       : resource_url,
        "txt_dir"   : txt_dir,
        "sql_dir"   : sql_dir,
        "state_file": state_file,
        "new_files_count" : len(new_files),
        })
    state.update(
            parse_text_files(logger, resource_name, new_files, txt_dir, sql_dir), 
            state_file)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1:
        loglevel = sys.argv[1]
    else:
        loglevel = "INFO"
    log.configure_logging()
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "main",
        "src"       : "30_pars.py"
        })
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(logger, m, config())
