from __future__ import print_function

import vertica_python as vp
import json


args = {
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'dbadmin',
    'password': '',
    'database': 'docker',
    # 10 minutes timeout on queries
    'read_timeout': 600,
    # default throw error on invalid UTF-8 results
    'unicode_error': 'strict',
    # SSL is disabled by default
    'ssl': False,
    # connection timeout is not enabled by default
    'connection_timeout': 5
}

## Ingest the data
#with open("args.json") as f:
#    args = json.load(f)
#conn = vp.connect(**args)
#cur = conn.cursor()
with vp.connect(**args) as conn:
    cur = conn.cursor()



#cur.execute("select * from stories")
#cur.fetchone()
#cur.fetchall()
#
with open("taggings.csv", "rb") as f:
    cur.copy("COPY lobsters.taggings from stdin DELIMITER ','", f)
with open("hidden.csv", "rb") as f:
    cur.copy("COPY lobsters.hiddens from stdin DELIMITER ','", f)
with open("stories.csv", "rb") as f:
    cur.copy("COPY lobsters.stories from stdin DELIMITER ','", f)
