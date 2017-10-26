"""

"""

import requests

BACKEND_ENDPOINT = "https://archive.kbb1.com/backend/content_units"
CONTENT_UNIT_URL = "https://archive.kbb1.com/en/programs/full/"
PAGE_SIZE = 100


def get_total():
    r = requests.get(BACKEND_ENDPOINT, params={'page_no': '1', 'page_size': '10', "language": 'en'})
    return r.json()['total']


def get_content_unts(page_no, page_size):
    r = requests.get(BACKEND_ENDPOINT, params={'page_no': str(page_no), 'page_size': str(page_size), "language": 'en'})
    return r.json()['content_units']


def get_cu_ids(content_units):
    cu_ids = []
    for cu in content_units:
        cu_ids.append(cu['id'])


def fetch_content_unit_files_data(cu_id, lang="en"):
    r = requests.get(BACKEND_ENDPOINT + "/" + cu_id, params={"language": lang})
    return r.json()['files']


def verify_content_units():
    total_pages = get_total() / PAGE_SIZE

    for _ in range(total_pages):
        # TODO
        pass
