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

conn = vp.connect(**args)
cur = conn.cursor()


cur.execute("drop table lobsters.taggings")
cur.execute("drop table lobsters.hiddens")
cur.execute("drop table lobsters.stories")
cur.execute("CREATE TABLE lobsters.taggings (id integer NOT NULL, story_id integer NOT NULL, tag_id integer NOT NULL)")
cur.execute("CREATE TABLE lobsters.hiddens (id integer NOT NULL, user_id integer NOT NULL, story_id integer NOT NULL)")
cur.execute("CREATE TABLE lobsters.stories ( \
  id integer NOT NULL, \
  created_at TIMESTAMP, \
  description varchar(4095), \
  hotness float, \
  markeddown_description varchar(4095), \
  short_id varchar(255), \
  title varchar(1023), \
  upvotes integer, \
  downvotes integer, \
  url varchar(255), \
  user_id integer)")

with open("taggings.csv", "rb") as f:
    cur.copy("COPY lobsters.taggings from stdin DELIMITER ','", f)
with open("hidden.csv", "rb") as f:
    cur.copy("COPY lobsters.hiddens from stdin DELIMITER ','", f)
with open("stories.csv", "rb") as f:
    cur.copy("COPY lobsters.stories from stdin DELIMITER ','", f)