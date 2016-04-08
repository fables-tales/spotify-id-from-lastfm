from db import database_connection
if __name__ == "__main__":
    f = open("lastfm.csv").readlines()
    conn = database_connection()
    for idx,line in enumerate(f):
        lfm_id = line.strip()
        cur = conn.cursor()
        cur.execute("insert into scrobbles (lastfm_id) values (%s)", (lfm_id,))
        if idx % 1024 == 0:
            conn.commit()
