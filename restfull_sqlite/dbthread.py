from Queue import Queue, Empty
from threading import Thread
import sqlite3 as sqlite
import collections


class DbThread(Thread):
    def __init__(self, dbname, timeout=None):
        Thread.__init__(self)
        self._timeout = timeout
        self._dbname = dbname
        self._db = None
        self._loop = True
        self._inq = Queue()

    def _execute(self, sql, params=None):
        cursor = self._db.cursor()
        if params is not None:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        ret = [x for x in cursor.fetchall()]
        cursor.close()
        return ret

    def execute(self, sql, params=None, timeout=None, nowait=False):
        outq = Queue()
        self._inq.put((sql, params, outq))
        if nowait:
            return None
        if timeout:
            ret = outq.get(timeout)
        elif self._timeout:
            ret = outq.get(self._timeout)
        else:
            ret = outq.get()
        outq.task_done()
        return ret

    def run(self):
        def dict_factory(cursor, row):
            d = collections.OrderedDict()
            for idx, col in enumerate(cursor.description):
                d[col[0].lower()] = row[idx]
            return d

        self._db = sqlite.connect(self._dbname, detect_types=sqlite.PARSE_DECLTYPES)
        self._db.text_factory = str
        self._db.row_factory = dict_factory

        while self._loop:
            try:
                sql, params, outq = self._inq.get(True, 1)
            except Empty as e:
                continue
            try:
                ret = self._execute(sql, params)
                outq.put(ret)
            except Exception as e:
                outq.put(e)
            self._inq.task_done()

        self._db.close()

    def close(self):
        self._loop = False


def test():
    print "TESTS"
    db = DbThread(':memory:')
    db.start()
    assert db.execute('CREATE TABLE prova (x INTEGER PRIMARY KEY ASC, y, z);') == []
    assert db.execute("INSERT INTO prova (y,z) VALUES (1,'cane');") == []
    assert db.execute("INSERT INTO prova (y,z) VALUES ('gatto',2);") == []
    assert db.execute("SELECT * FROM prova;") == [{'y': 1, 'x': 1, 'z': 'cane'}, {'y': 'gatto', 'x': 2, 'z': 2}]
    assert db.execute("SELECT * FROM prova WHERE y=?;", ('gatto',)) == [{'y': 'gatto', 'x': 2, 'z': 2}]

    assert db.execute("INSERT INTO prova (y,z) VALUES (1,'cane');", nowait=True) is None

    db.close()
    print "OK"


if __name__ == "__main__":
    test()
