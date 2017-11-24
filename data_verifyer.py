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

BACKEND_ENDPOINT_CU = "https://archive.kbb1.com/backend/content_units"
BACKEND_ENDPOINT_COLLECTIONS = "https://archive.kbb1.com/backend/collections"
CDN_ENDPOINT = "https://cdn.kabbalahmedia.info/"
CONTENT_UNIT_URL = "https://archive.kbb1.com/en/programs/full/"
PAGE_SIZE = 1000

stop_event = None


def get_total(endpoint):
    r = requests.get(endpoint, params={'page_no': '1', 'page_size': '10', "language": 'en'})
    return r.json()['total']


def fetch_data_units(endpoint, page_no=1, page_size=PAGE_SIZE):
    r = requests.get(endpoint, params={'page_no': str(page_no), 'page_size': str(page_size), "language": 'en'})
    try:
        return r.json()['content_units']
    except KeyError:
        return r.json()['collections']


def fetch_all_data_units(logger, endpoint, data_queue):
    """

    :param endpoint:
    :param data_queue: queue
    :param logger: logging
    """
    total_items = get_total(endpoint)
    total_pages = total_items // PAGE_SIZE

    logger.info("{} items found. Going to process {} pages {} elements per page".format(total_items, total_pages,
                                                                                        PAGE_SIZE))

    for page in range(1, total_pages):
        logger.info("Processing page #{}".format(page))
        try:
            for data_unit in fetch_data_units(endpoint, page, PAGE_SIZE):
                data_queue.put(data_unit['id'])
        except Exception as err:
            logger.error("{} on wile fetching page #{}".format(err.args, page))
            pass


def fetch_content_units_data_worker(logger, endpoint, coll_queue, lang="en"):
    """

    :param coll_queue: str
    :param endpoint: str
    :param logger: logging
    :param lang: str
    :return: int
    """
    global stop_event
    cu_id = None
    r = None

    while not stop_event.is_set():
        try:
            cu_id = coll_queue.get_nowait()
            r = requests.get(endpoint + "/" + cu_id, params={"language": lang})
            logger.info("fetched: {}:{}".format(r.json()['id'], r.json()['content_type']))
            if r.json()['id'] != cu_id:
                logger.error('Fetched cuid {} doesn\'t match requested cuid {}'.format(cu_id, r.json()['id']))
        except KeyError:
            logger.error('Bad JOSN from content unit! ID not found: {} - {}'.format(cu_id, r.json()))
        except queue.Empty:
            logger.info("Done processing cuid's queue")
            stop_event.set()
        except Exception as err:
            logger.error("Unhandled error {} in {} - id {}".format(err, sys.exc_info()[-1].tb_lineno, r.json()['id']))
    return r.status_code


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threads", help="Number of fetching threads", type=int, default=0)
    args = parser.parse_args()
    return args


def main():
    global stop_event

    cuid_queue = queue.Queue()
    collections_queue = queue.Queue()
    stop_event = Event()

    logger = Logger().logger
    args = get_args()
    threads = args.threads
    if threads == 0:
        threads = (os.cpu_count() or 1) * 5

    logger.info("Fetching all content units ....")
    fetch_all_data_units(logger, BACKEND_ENDPOINT_CU, cuid_queue)
    logger.info("Done fetching content units list")

    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="fetcher") as cu_fetchers_pool:
        for _ in range(threads):
            cu_fetchers_pool.submit(fetch_content_units_data_worker, logger, BACKEND_ENDPOINT_CU, cuid_queue)

    stop_event.clear()
    logger.info("Fetching all collections ....")
    fetch_all_data_units(logger, BACKEND_ENDPOINT_COLLECTIONS, collections_queue)
    logger.info("Done fetching collections list")

    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="fetcher") as fetchers_pool:
        for _ in range(threads):
            fetchers_pool.submit(fetch_content_units_data_worker, logger, BACKEND_ENDPOINT_COLLECTIONS,
                                 collections_queue)


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
