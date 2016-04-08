import requests
import logging
from multiprocessing import Pool
import sys
from bs4 import BeautifulSoup
import os
import itertools

def grouper(iterable, n, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)


logging.basicConfig(stream=sys.stdout, level=logging.WARN, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def resume(lastfm_ids):
    lines_passed = 0
    if os.path.isfile(sys.argv[2]):
        file_lines = open(sys.argv[2]).readlines()
        last_line = None
        for last_line in file_lines:
            pass

        last_lastfm_id = last_line.split(",")[0]
        logging.warn("Resuming lastfm pull with lastfm id %s", last_lastfm_id)

        lastfm_id = None
        while lastfm_ids[0].strip() != last_lastfm_id:
            lines_passed += 1
            lastfm_ids = lastfm_ids[1:]
    else:
        logging.warn("Starting new lastfm scrape")

    return lines_passed

def report_progress(offset, lines_in_target):
    logging.warn("Progress: processed: %d\ttotal: %d\tpercent complete: %f" % (offset, lines_in_target, offset*100.0/lines_in_target))


def spotify_id_from_lastfm_page(url):

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
            spotify_id = "null"
    else:
        spotify_id = "null"

    return spotify_id

def build_line_from_id(lastfm_id):
    lastfm_id = lastfm_id.strip()
    url = "http://last.fm/%s" % (lastfm_id)
    spotify_id = spotify_id_from_lastfm_page(url)
    return lastfm_id + "," + spotify_id + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: python spifl.py input_file output_file"
        sys.exit(1)

    lines_in_target = len(open(sys.argv[1]).readlines())

    lastfm_ids = open(sys.argv[1]).readlines()
    offset = resume(lastfm_ids)

    group_size = 8

    p = Pool(group_size)

    with open(sys.argv[2], "a") as writer:
        for group in grouper(lastfm_ids, group_size):
            group = list(group)
            lines = p.map(build_line_from_id, group)
            for line in lines:
                writer.write(line)
                writer.flush()
                offset += 1

            if offset % 100 == 0:
                report_progress(offset, lines_in_target)
