__author__ = "roberto"

DELETE_FROM = "DELETE FROM `%s` WHERE (%s);"
WHERE = " WHERE (%s) "
AND = " AND "
SELECT_ALL_FROM = "SELECT * FROM `%s`"
CREATE_TABLE = "CREATE TABLE `%s` (%s);"
PRAGMA_TABLE_INFO = "PRAGMA table_info(`%s`);"
UPDATE = "UPDATE `%s` SET %s WHERE (%s);"
DROP_TABLE = "DROP TABLE `%s`;"
LIST_TABLE = "SELECT name FROM sqlite_master WHERE type='table';"
INSERT_INTO = "INSERT INTO `%s` (%s) VALUES (%s);"

OPERATOR_MAP = {
    "eq": "=?",
    "gt": ">?",
    "lt": "<?",
    "gte": ">=?",
    "lte": "<=?",
    "gteq": ">=?",
    "lteq": "<=?",
    "isnotnull": " IS NOT NULL ",
    "isnull": " IS NOT NULL ",
    "like": " LIKE ? "
    # @todo aggiungere operatori -- add more operators
}


def _create_where(conditions, values):
    _where = []
    for field in conditions:
        for operator, value in conditions[field].items():
            sql_operator = OPERATOR_MAP[operator]
            _where.append("`%s`%s" % (field, sql_operator))
            if "?" in sql_operator:
                values.append(value)
    return _where


def drop_table(tablename):
    return DROP_TABLE % tablename


def list_table():
    return LIST_TABLE


def insert(tablename, data):
    values = data.values()
    fields = " , ".join([k for k, v in data.items()])
    markplace = " , ".join(["?" for k, v in data.items()])
    sql = INSERT_INTO % (tablename, fields, markplace)
    return sql, values


def update(tablename, conditions, data):
    values = data.values()
    _set = ",".join(["`%s`=?" % k for k in data])
    _where = _create_where(conditions, values)
    _where = AND.join(_where)
    sql = UPDATE % (tablename, _set, _where)
    return sql, values


def table_info(tablename):
    return PRAGMA_TABLE_INFO % tablename


def create_table(tablename, data):
    fields = " , ".join(["`" + k + "` " + v for k, v in data.items()])
    sql = CREATE_TABLE % (tablename, fields)
    return sql


def select(tablename, conditions):
    sql = SELECT_ALL_FROM % tablename
    values = []
    _where = _create_where(conditions, values)
    if _where:
        _where = AND.join(_where)
        sql += WHERE % _where
    sql += ";"
    return sql, values


def delete(tablename, conditions):
    values = []
    _where = _create_where(conditions, values)
    _where = AND.join(_where)
    sql = DELETE_FROM % (tablename, _where)
    return sql, values


def test():
    values = []
    assert _create_where({"y": {"eq": "gatto"}}, values) == ["`y`=?"]
    assert values == ["gatto"]


if __name__ == "__main__":
    test()
