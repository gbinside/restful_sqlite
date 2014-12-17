from flask import Flask, request
from dbthread import DbThread
import collections
import sql_helper
import json
import sys


DATABASE = ':memory:'

app = Flask(__name__)

# UTILS


def get_data():
    data = request.data
    if not data:
        data = request.get_data()
    if not data:
        try:
            data = request.form.keys()[0]
        except IndexError:
            data = '{}'
    return data


# DEBUG

@app.route("/debug", methods=['GET', 'POST', 'PUT', 'DELETE'])
def debug():
    ret = []
    for k in dir(request):
        try:
            ret.append(('[%s] => ' % k) + repr(getattr(request, k)))
        except TypeError as e:
            ret.append('[%s] => TypeError' % k)
    print '*' * 79
    print '\n'.join(ret)
    print '*' * 79
    data = get_data()
    print 'data => ', repr(data)
    print '*' * 79
    return '\n'.join(ret)


# API V1/V2

@app.route("/v2", methods=['GET'])
@app.route("/v1", methods=['GET'])
def list_tables():
    """
    list tables

    usage:


    """
    ret = app.db.execute(sql_helper.list_table())
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(ret)


@app.route('/v2/<tablename>/schema', methods=['DELETE'])
@app.route('/v1/<tablename>/drop')
def drop_table(tablename):
    ret = app.db.execute(sql_helper.drop_table(tablename))
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(ret)


@app.route('/v2/<tablename>', methods=['POST'])
@app.route('/v1/<tablename>/insert', methods=['POST'])
def insert_into_table(tablename):
    """
    insert a record into a table

    usage:


    """
    data = json.loads(get_data())
    sql, values = sql_helper.insert(tablename, data)

    ret = app.db.execute(sql, values)
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(ret)


@app.route('/v2/<tablename>', methods=['PUT'])
@app.route('/v1/<tablename>/update', methods=['POST'])
def update_table(tablename):
    post_data = json.loads(get_data())

    data = post_data['data']
    conditions = post_data['conditions']
    sql, values = sql_helper.update(tablename, conditions, data)

    ret = app.db.execute(sql, values)
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(ret)


@app.route('/v2/<tablename>/schema', methods=['GET'])
@app.route('/v1/<tablename>', methods=['GET'])
def get_table_schema(tablename):
    """
    get the table schema

    usage:


    """
    ret = app.db.execute(sql_helper.table_info(tablename))
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(ret)


@app.route('/v2/<tablename>/schema', methods=['POST'])
@app.route('/v1/<tablename>', methods=['POST'])
def create_table(tablename):
    """
    create a table

    usage:


    """
    data = json.loads(get_data(), object_pairs_hook=collections.OrderedDict)
    sql = sql_helper.create_table(tablename, data)
    ret = app.db.execute(sql)
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(ret)


@app.route('/v2/<tablename>', methods=['GET'])
@app.route('/v1/<tablename>/select', methods=['GET', 'POST'])
def select_from_table(tablename):
    """
    select FORM table

    usage:

    """
    post_data = json.loads(get_data())
    conditions = post_data.get('conditions', {})

    sql, values = sql_helper.select(tablename, conditions)
    if values:
        ret = app.db.execute(sql, values)
    else:
        ret = app.db.execute(sql)
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(e)



@app.route('/v2/<tablename>', methods=['DELETE'])
@app.route('/v1/<tablename>/delete', methods=['POST'])
def delete_from_table(tablename):
    """
    select FORM table

    usage:

    """
    post_data = json.loads(get_data())
    conditions = post_data['conditions']

    sql, values = sql_helper.delete(tablename, conditions)
    ret = app.db.execute(sql, values)
    try:
        return json.dumps(ret)
    except TypeError as e:
        return str(e)


#                .__
#   _____ _____  |__| ____
#  /     \\__  \ |  |/    \
# |  Y Y  \/ __ \|  |   |  \
# |__|_|  (____  /__|___|  /
#       \/     \/        \/

if __name__ == "__main__":
    app.db = DbThread(DATABASE, timeout=10)
    app.db.start()
    app.run(host='0.0.0.0', debug='debug' in sys.argv, port=8080)

