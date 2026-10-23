import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path


def init(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, device_id TEXT, event_time TEXT, temperature REAL);
    CREATE TABLE IF NOT EXISTS dead_letter(payload TEXT, reason TEXT);''')

def consume(db, lines):
    init(db)
    with db:
        for line in lines:
            try:
                item=json.loads(line)
                datetime.fromisoformat(item['event_time'].replace('Z','+00:00'))
                value=float(item['temperature'])
                if not item['event_id'] or not item['device_id']:
                    raise ValueError('empty identifier')
                db.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?)',(item['event_id'],item['device_id'],item['event_time'],value))
            except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
                db.execute('INSERT INTO dead_letter VALUES (?,?)',(line,str(exc)))

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',default='data/events.jsonl');p.add_argument('--db',default='output/events.db');a=p.parse_args()
    Path(a.db).parent.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(a.db) as db:
        consume(db,Path(a.input).read_text().splitlines())
        print('events:',db.execute('SELECT COUNT(*) FROM events').fetchone()[0]);print('dead letters:',db.execute('SELECT COUNT(*) FROM dead_letter').fetchone()[0])
if __name__=='__main__':main()
