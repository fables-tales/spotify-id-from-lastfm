import requests
import logging
from multiprocessing import Pool
import sys
from bs4 import BeautifulSoup
import os
import itertools
from db import database_connection

logging.basicConfig(stream=sys.stdout, level=logging.WARN, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def report_progress(offset, lines_in_target):
    logging.warn("Progress: processed: %d\ttotal: %d\tpercent complete: %f" % (offset, lines_in_target, offset*100.0/lines_in_target))


def spotify_id_from_lastfm_page(url):
    try:
        try:
            response = requests.get(url).text
        except requests.exceptions.ConnectionError:
            # bad retry
            response = requests.get(url).text

        soup = BeautifulSoup(response, "html.parser")

        maybe_spotify_id = soup.findAll("a", {"class": "image-overlay-playlink-link"})

        if len(maybe_spotify_id) > 0:
            try:
                spotify_id = maybe_spotify_id[0].attrs["data-spotify-id"]
            except KeyError:
                spotify_id = None
        else:
            spotify_id = None

        return spotify_id
    except KeyboardInterrupt:
        exit(1)

def build_line_from_id(lastfm_id):
    lastfm_id = lastfm_id.strip()
    url = "http://last.fm/%s" % (lastfm_id)
    spotify_id = spotify_id_from_lastfm_page(url)
    return (lastfm_id, spotify_id)

def update_ids(ids):
    conn = database_connection()
    for (lastfm_id, spotify_id) in ids:
        cur = conn.cursor()
        cur.execute("update scrobbles set spotify_id=%s, has_spotify_id=%s where lastfm_id=%s", (spotify_id, True, lastfm_id))
        conn.commit()

def get_lastfm_ids_without_spotify(group_size):
    conn = database_connection()
    cur = conn.cursor()
    cur.execute("select lastfm_id from scrobbles where has_spotify_id=false limit %s" % group_size)

    return [x[0] for x in cur]

def get_total_scrobbles():
    conn = database_connection()
    cur = conn.cursor()
    cur.execute("select count(*) from scrobbles")
    return cur.next()[0]

def with_ids():
    conn = database_connection()
    cur = conn.cursor()
    cur.execute("select count(*) from scrobbles where has_spotify_id=true")
    return cur.next()[0]


if __name__ == "__main__":
    group_size = 8
    p = Pool(group_size)
    lines_in_target = get_total_scrobbles()

    have_group = True
    group = get_lastfm_ids_without_spotify(group_size)
    offset = 0
    while len(group) > 0:
        group = get_lastfm_ids_without_spotify(group_size)
        lines = p.map(build_line_from_id, group)
        update_ids(lines)

        offset += len(lines)

        if offset % (group_size*4) == 0:
            report_progress(with_ids(), lines_in_target)
