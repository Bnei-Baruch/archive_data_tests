#!/usr/bin/env python3
"""

"""
import argparse
import traceback
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from logger import Logger

import os
import requests
import queue

import sys

BACKEND_ENDPOINT = "https://archive.kbb1.com/backend/content_units"
CDN_ENDPOINT = "https://cdn.kabbalahmedia.info/"
CONTENT_UNIT_URL = "https://archive.kbb1.com/en/programs/full/"
PAGE_SIZE = 1000

stop_event = None


def get_total():
    r = requests.get(BACKEND_ENDPOINT, params={'page_no': '1', 'page_size': '10', "language": 'en'})
    return r.json()['total']


def fetch_content_units(page_no=1, page_size=PAGE_SIZE):
    r = requests.get(BACKEND_ENDPOINT, params={'page_no': str(page_no), 'page_size': str(page_size), "language": 'en'})
    return r.json()['content_units']


def fetch_all_content_units(logger, cuid_queue):
    """

    :param cuid_queue: queue
    :param logger: logging
    """
    total_pages = get_total() // PAGE_SIZE

    for page in range(1, total_pages):
        print("--------------- PAGE: {} -----------------".format(page))
        try:
            for cu_id in fetch_content_units(page, PAGE_SIZE):
                cuid_queue.put(cu_id['id'])
        except Exception as err:
            logger.error("{} on wile fetching page #{}".format(err, page))
            pass


def fetch_content_units_data_worker(logger, cuid_queue, lang="en"):
    """

    :param logger: logging
    :param cuid_queue: str
    :param lang: str
    :return: int
    """
    global stop_event
    cu_id = None
    r = None

    while not stop_event.is_set():
        try:
            cu_id = cuid_queue.get_nowait()
            r = requests.get(BACKEND_ENDPOINT + "/" + cu_id, params={"language": lang})
            logger.info("fetched: {}:{}".format(r.json()['id'], r.json()['content_type']))
            if r.json()['id'] != cu_id:
                logger.error('Fetched cuid {} doesn\'t match requested cuid {}'.format(cu_id, r.json()['id']))
        except KeyError:
            logger.error('Bad JOSN from content unit! ID not found: {} - {}'.format(cu_id, r.json()))
        except queue.Empty:
            logger.info("Done processing cuid's queue")
            stop_event.set()
        except Exception as err:
            logger.error("Unhandled error {} in {}".format(err, sys.exc_info()[-1].tb_lineno))
    return r.status_code


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threads", help="Number of fetching threads", type=int, default=0)
    parser.add_argument("-r", "--recover", help="Recover and continue run, based on history", type=str)
    args = parser.parse_args()
    return args


def main():
    global stop_event

    cuid_queue = queue.Queue()
    stop_event = Event()

    logger = Logger().logger
    args = get_args()
    threads = args.threads
    logger.info("Fetching all content units ....")
    fetch_all_content_units(logger, cuid_queue)
    logger.info("Done fetching content units list")

    if threads == 0:
        threads = (os.cpu_count() or 1) * 5

    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="cu_fetcher") as cu_fetchers_pool:
        for _ in range(threads):
            cu_fetchers_pool.submit(fetch_content_units_data_worker, logger, cuid_queue)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Test stopped by user ...")
        sys.exit(0)
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
    finally:
        stop_event.set()
