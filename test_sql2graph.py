from sql2graph import SQLParser


# def test_get_databases_names_in_simple_query():
#     sql_query = 'select title from cms.post;'
#     parsed = SQLParse(sql_query)
#     assert parsed.uses_database() == ['cms']


# def test_get_databases_names_in_complex_query():
#     sql_query = """
#         SELECT option_value
#          FROM `database1`.`wp_options`
#          WHERE option_name="active_plugins"
#         UNION
#         SELECT option_value
#          FROM `database2`.`wp_options`
#          WHERE option_name="active_plugins"
#         """
#     parsed = SQLParse(sql_query)
#     assert parsed.uses_database() == ['database1', 'database2']

def test_get_undefined_database():
    sql_query = 'select * from table_name;'

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = []

    assert actual == expected


def test_get_single_database():
    sql_query = 'select * from db1.table_name;'

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = ['db1']

    assert actual == expected


def test_get_multiple_databases():
    sql_query = 'select * from table_name;'

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = []

    assert actual == expected


def test_get_multiple_databases_on_complex_query():
    sql_query = """
    SELECT mydatabase1.tblUsers.UserID,
        mydatabse2.tblUsers.UserID
        FROM
        mydatabase1.tblUsers
        INNER JOIN mydatabase2.tblUsers
           ON mydatabase1.tblUsers.UserID = mydatabase2.tblUsers.UserID"""

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = ['mydatabase1', 'mydatabase2']

    assert actual == expected
