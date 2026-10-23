import sqlite3,json
from src.stream import consume

def test_dedup_and_dead_letter():
    db=sqlite3.connect(':memory:')
    e=json.dumps(dict(event_id='x',device_id='d',event_time='2026-01-01T00:00:00Z',temperature=4))
    consume(db,[e,e,'not json'])
    assert db.execute('SELECT COUNT(*) FROM events').fetchone()[0]==1
    assert db.execute('SELECT COUNT(*) FROM dead_letter').fetchone()[0]==1
