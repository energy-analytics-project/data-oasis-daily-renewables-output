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
# 10_down.py : download resources from resource_url
# 
# * after downloading, resource urls are appended to zip/downloaded.txt
# * zip/downloaded.txt can be checked into the repo, whereas the the downloaded
#   resources should not be checked in to git. Instead, they are uploaded to
#   an S3 bucket 'eap'.
# -----------------------------------------------------------------------------

import glob
import datetime
import requests
import sys
import os
import time
import logging
import json
import shutil
import zipfile
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE, S_IWGRP, S_IWOTH
from edl.resources import log
from edl.resources import state
from edl.resources import time as xtime
from edl.resources import web


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def config():
    """
    config = {
            "source_dir"    : download .txt files here
            "working_dir"   : zip .txt files and copy here 
            "state_file"    : fqpath to file that lists downloaded zip files
            }
    """
    cwd                     = os.path.abspath(os.path.curdir)
    zip_dir                 = os.path.join(cwd, "zip")
    txt_dir                 = os.path.join(cwd, "txt")
    state_file              = os.path.join(zip_dir, "state.txt")
    config = {
            "working_dir"   : zip_dir,
            "source_dir"    : txt_dir,
            "state_file"    : state_file
            }
    return config

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(logger, manifest, config):
    start_date      = datetime.date(*manifest['start_date'])
    resource_name   = manifest['name']
    resource_url    = manifest['url']
    delay           = manifest['download_delay_secs']
    download_dir    = config['working_dir']
    txt_dir         = config['source_dir']
    state_file      = config['state_file']
    # sleep for N seconds in between downloads to meet caiso expected use requirements
    dates   = xtime.range_pairs(xtime.day_range_to_today(start_date))
    urls    = list(web.generate_urls(logger, dates, resource_url))
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "run",
        "resource"  : resource_name,
        "url"       : resource_url,
        "delay"     : delay,
        "download_dir": download_dir,
        "state_file": state_file,
        "start_date": str(start_date),
        "urls_count": len(urls),
        })

    # download .txt files
    downloaded_txt_urls = web.download(
        logger,
        resource_name,
        delay,
        urls,
        state_file,
        download_dir,
        ending='.txt')

    # copy .txt files to ./text dir and then
    # compress original .txt files to .zip files
    if not os.path.exists(txt_dir):
        log.debug(logger, {
            "name"      : __name__,
            "method"    : "run",
            "src"       : "10_down.py",
            "message"   : "created target txt dir: %s" % txt_dir,
            })
        os.makedirs(txt_dir)

    # process downloaded .txt files
    data_files = glob.glob(os.path.join(download_dir, "*DailyRenewablesWatch.txt"))
    for tf in data_files:
        try:
            # remove write protections for .txt files
            os.chmod(os.path.join(download_dir, tf), S_IWRITE|S_IWGRP|S_IWOTH|S_IREAD|S_IRGRP|S_IROTH)
            
            # if the txt file is here, it needs to be copied to the ./txt dir
            fqtf = os.path.join(download_dir, tf)
            fqtf2 = os.path.join(txt_dir, tf)
            fqtfzip = os.path.join(download_dir, '%s.zip' % tf)
            if not os.path.exists(fqtf2):
                shutil.copyfile(fqtf, fqtf2)
            with zipfile.ZipFile(fqtfzip, 'w') as myzip:
                myzip.write(fqtfzip)

            # set .zip file to be read only
            os.chmod(fqtfzip, S_IREAD|S_IRGRP|S_IROTH)
        
            # remove the zip/.txt file as it's been copied to txt/.txt
            if os.path.exists(fqtf2) and os.path.exists(fqtfzip):
                os.remove(fqtf)
            log.debug(logger, {
                "name"      : __name__,
                "method"    : "run",
                "src"       : "10_down.py",
                "message"   : "zipped file: %s" % tf,
                })
        except Exception as e:
            log.error(logger, {
                "name"      : __name__,
                "method"    : "run",
                "src"       : "10_down.py",
                "file"      : tf,
                "error"     : "failed to process file",
                "exception" : str(e),
                })


    # TODO: something is clobbering perms on the state file, so clobber it back
    os.chmod(os.path.join(download_dir, 'state.txt'), S_IWRITE|S_IWGRP|S_IWOTH|S_IREAD|S_IRGRP|S_IROTH)
    # final step
    state.update(downloaded_txt_urls, state_file)
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
        "src"       : "10_down.py"
        })
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(logger, m, config())
