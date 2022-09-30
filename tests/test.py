from collections import OrderedDict
import urllib
import json
import sys


def test_v1():
    # Interface 1 - only POST and GET

    # delete
    # [urllib.urlopen('http://localhost:8080/debug').read() for x in xrange(10)]

    urllib.urlopen('http://localhost:8080/v1/actor/drop').read()
    urllib.urlopen('http://localhost:8080/v1/temptable/drop').read()

    # create table
    assert urllib.urlopen('http://localhost:8080/v1/temptable',
                          json.dumps(
                              OrderedDict([('id', 'INTEGER PRIMARY KEY ASC'), ('x', ''), ('y', ''), ('z', '')])
                          )).read() == '[]'

    # list tables
    assert urllib.urlopen('http://localhost:8080/v1').read() == '[{"name": "temptable"}]'

    # insert into
    assert urllib.urlopen('http://localhost:8080/v1/temptable/insert', json.dumps({'y': 'cane', 'z': 1})).read() == '[]'
    assert urllib.urlopen('http://localhost:8080/v1/temptable/insert',
                          json.dumps({'y': 1, 'z': 'gatto'})).read() == '[]'

    # select
    assert urllib.urlopen(
        'http://localhost:8080/v1/temptable/select').read() == \
           '[{"id": 1, "x": null, "y": "cane", "z": 1}, {"id": 2, "x": null, "y": 1, "z": "gatto"}]'

    # update
    assert urllib.urlopen('http://localhost:8080/v1/temptable/update', json.dumps({
        'conditions': {
            'z': {'eq': 'gatto'}
        },
        'data': {
            'z': 2,
            'y': 'gatto'
        }
    })).read() == '[]'

    # select
    assert urllib.urlopen(
        'http://localhost:8080/v1/temptable/select').read() == '[{"id": 1, "x": null, "y": "cane", "z": 1}, {"id": 2, "x": null, "y": "gatto", "z": 2}]'
    assert urllib.urlopen('http://localhost:8080/v1/temptable/select', json.dumps({
        'conditions': {
            'z': {'eq': 1}
        },
        'limit': 1
    })).read() == '[{"id": 1, "x": null, "y": "cane", "z": 1}]'

    # delete
    assert urllib.urlopen('http://localhost:8080/v1/temptable/delete', json.dumps({
        'conditions': {
            'z': {'eq': 1}
        },
    })).read() == '[]'

    # select
    assert urllib.urlopen('http://localhost:8080/v1/temptable/select', json.dumps({
        'conditions': {
            'z': {'eq': 1}
        },
    })).read() == '[]'

    assert urllib.urlopen(
        'http://localhost:8080/v1/temptable').read() == \
           '[{"cid": 0, "name": "id", "type": "INTEGER", "notnull": 0, "dflt_value": null, "pk": 1}, ' \
           '{"cid": 1, "name": "x", "type": "", "notnull": 0, "dflt_value": null, "pk": 0}, ' \
           '{"cid": 2, "name": "y", "type": "", "notnull": 0, "dflt_value": null, "pk": 0}, ' \
           '{"cid": 3, "name": "z", "type": "", "notnull": 0, "dflt_value": null, "pk": 0}]'

    assert urllib.urlopen('http://localhost:8080/v1/temptable/drop').read() == '[]'


def test_v2():
    # Interface 2 - POST, GET, DELETE and PUT
    import urllib2

    def urlopen(url, data=None, method='GET'):
        content_header = {'Content-type': 'application/json',
                          'Accept': 'application/vnd.error+json,application/json',
                          'Accept-Version': '1.0'}
        if data:
            request = urllib2.Request(url=url, data=json.dumps(data), headers=content_header)
        else:
            request = urllib2.Request(url=url)
        request.get_method = lambda: method

        response = urllib2.urlopen(request)

        return response

    # print urlopen('http://localhost:8080/debug', method='PUT').read()

    urlopen('http://localhost:8080/v2/actor/schema', method='DELETE').read() == '[]'
    urlopen('http://localhost:8080/v2/temptable/schema', method='DELETE').read() == '[]'


    assert urlopen('http://localhost:8080/v2/actor/schema',
                   OrderedDict(
                       [('id', 'INTEGER PRIMARY KEY ASC'), ('x', ''), ('y', ''), ('z', '')]
                   ),
                   'POST').read() == '[]'

    assert urlopen('http://localhost:8080/v2/actor', {'x': None, 'y': 'cane', 'z': 1}, 'POST').read() == '[]'  # insert
    assert urlopen('http://localhost:8080/v2/actor', {'x': 1, 'y': 'furetto', 'z': 2}, 'POST').read() == '[]'  # insert
    assert urlopen('http://localhost:8080/v2/actor', {'x': 0, 'y': 'gatto', 'z': 3}, 'POST').read() == '[]'  # insert

    assert urlopen('http://localhost:8080/v2/actor', {
        'conditions': {
            'z': {'eq': 1}
        },
        'limit': 1
    }).read() == '[{"id": 1, "x": null, "y": "cane", "z": 1}]'  # GET

    assert urlopen('http://localhost:8080/v2/actor', {
        'conditions': {
            'y': {'eq': 'gatto'}
        },
        'data': {
            'z': -2,
            'y': 'gatto'
        }
    }, 'PUT').read() == '[]'

    assert urlopen('http://localhost:8080/v2/actor', {
        'conditions': {
            'y': {'eq': 'gatto'}
        },
    }, 'GET').read() == '[{"id": 3, "x": 0, "y": "gatto", "z": -2}]'

    assert urlopen('http://localhost:8080/v2/actor', {
        'conditions': {
            'y': {'eq': 'gatto'}
        },
    }, 'DELETE').read() == '[]'

    assert urlopen('http://localhost:8080/v2/actor', {
        'conditions': {
            'y': {'eq': 'gatto'}
        },
    }, 'GET').read() == '[]'

    assert urlopen('http://localhost:8080/v2/actor/schema', method='DELETE').read() == '[]'


def test(argv):
    import inspect

    my_name = inspect.stack()[0][3]
    for f in argv:
        globals()[f]()
    if not argv:
        fs = [globals()[x] for x in globals() if
              inspect.isfunction(globals()[x]) and x.startswith('test') and x != my_name]
        for f in fs:
            print(f.__name__)
            f()


if __name__ == '__main__':
    sys.exit(test(sys.argv[1:]))