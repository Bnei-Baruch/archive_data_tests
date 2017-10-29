#!/usr/bin/env python3
"""

"""

import requests
import logging

import sys

from urllib3.exceptions import ProtocolError

BACKEND_ENDPOINT = "https://archive.kbb1.com/backend/content_units"
CDN_ENDPOINT = "https://cdn.kabbalahmedia.info/"
CONTENT_UNIT_URL = "https://archive.kbb1.com/en/programs/full/"
PAGE_SIZE = 1000


def get_total():
    r = requests.get(BACKEND_ENDPOINT, params={'page_no': '1', 'page_size': '10', "language": 'en'})
    return r.json()['total']


def fetch_content_units(page_no=1, page_size=PAGE_SIZE):
    r = requests.get(BACKEND_ENDPOINT, params={'page_no': str(page_no), 'page_size': str(page_size), "language": 'en'})
    return r.json()['content_units']


def fetch_cu_ids(content_units):
    cu_ids = []
    if not content_units:
        raise ValueError("content_units are empty")
    for cu in content_units:
        cu_ids.append(cu['id'])
    return cu_ids


def fetch_content_unit_files_data(cu_id, lang="en"):
    if not cu_id:
        raise ValueError("Emtpy content unit ID")
    r = requests.get(BACKEND_ENDPOINT + "/" + cu_id, params={"language": lang})
    return r.status_code, r.json()['files']


def check_cdn_file_url(file_id):
    # first we need to get the real url to file we're redirected to
    file_url = CDN_ENDPOINT + file_id
    r = requests.get(file_url, allow_redirects=False)
    # getting real url and just trying to access without downloading it
    file_url = r.headers['location']
    r = requests.get(file_url, stream=True)
    return r.status_code, file_url


def test_fetch_all_content_units(logger):
    total_pages = get_total() // PAGE_SIZE
    cu_ids = []

    for page in range(1, total_pages):
        print("--------------- PAGE: {} -----------------".format(page))
        try:
            for cu_id in fetch_content_units(page, PAGE_SIZE):
                cu_ids.append(cu_id['id'])
        except ConnectionError as cerr:
            logger.error("{} on wile fetching page #{}".format(cerr.strerror, page))
            pass
    return cu_ids


def test_fetch_content_units_file_data(logger, cu_ids):
    file_guids = []
    for cu_id in cu_ids:
        try:
            status_code, files_data = fetch_content_unit_files_data(cu_id)
            if status_code != 200:
                logger.error("Error accessing {}{} - status code: {}".format(BACKEND_ENDPOINT, cu_id, status_code))
                continue
            for file in files_data:
                print("File: {}".format(file))
                file_guids.append(file)
        except ConnectionResetError as cerr:
            logger.error("{} while fetching content unit {}".format(cerr, BACKEND_ENDPOINT + "/" +
                                                                    cu_id))
            continue
    return file_guids


def run_content_units_test(logger):
    total_pages = get_total() // PAGE_SIZE

    for page in range(1, total_pages):
        print("--------------- PAGE: {} -----------------".format(page))
        try:
            cu_ids = fetch_cu_ids(fetch_content_units(page, PAGE_SIZE))
            for cu_id in cu_ids:
                try:
                    _, files_data = fetch_content_unit_files_data(cu_id)
                except ConnectionResetError as cerr:
                    logger.error("{} while fetching content unit {}".format(cerr.strerror, BACKEND_ENDPOINT + "/" +
                                                                            cu_id))
                    continue
                for file in files_data:
                    status_code, file_url = check_cdn_file_url(file['id'])
                    if status_code != 200:
                        logger.error("Error accessing {} - status code: {}".format(file_url, status_code))
                    print("File: {} - URL: {} - Status code: {}".format(file['name'], file_url, status_code))
        except ConnectionResetError as cerr:
            logger.error("{} on wile fetching page #{}".format(cerr.strerror, page))
            pass


def main():
    logger = logging.getLogger("__main__")
    hdlr = logging.FileHandler('error.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)

    tests = {
        "fetch_countent_units": fetch_content_units()
    }
    run_content_units_test(logger)
    #cu_ids = test_fetch_all_content_units(logger)
    #test_fetch_content_units_file_data(logger, cu_ids)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Test stopped by user ...")
        sys.exit(0)
