from sql2graph import SQLParser


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
    sql_query = """SELECT *
        FROM database_2.table_2
        JOIN database_1.table_1
            ON (database_2.table_2.some_field = database_1.table_1.some_other_field)
        WHERE database_1.table_1.data_1 LIKE database_2.table_2.data_2;"""

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = ['database_2', 'database_1']

    assert set(actual) == set(expected)


def test_get_multiple_databases_on_complex_query():
    sql_query = """
    SELECT mydatabase1.tblUsers.UserID,
        mydatabse2.tblUsers.UserID
        FROM
        mydatabase1.tblUsers
        INNER JOIN mydatabase2.tblUsers
           ON mydatabase1.tblUsers.UserID = mydatabase2.tblUsers.UserID;"""

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = ['mydatabase1', 'mydatabase2']

    assert set(actual) == set(expected)


def test_get_single_table():
    sql_query = 'select * from tablita;'

    parser = SQLParser(sql_query)
    actual = parser.get_tables()
    expected = [('tablita', None)]

    assert actual == expected


def test_get_tables_with_alias():
    sql_query = """SELECT e.last_name,
        e.department_id,
        d.department_name
        FROM   employees e
        LEFT OUTER JOIN department d
            ON ( e.department_id = d.department_id ); """

    parser = SQLParser(sql_query)
    actual = parser.get_tables()
    expected = [('employees', 'e'), ('department', 'd')]

    assert set(actual) == set(expected)


def test_get_tables_from_multiple_databases():
    sql_query = """SELECT *
        FROM database_2.table_2
        JOIN database_1.table_1
            ON (database_2.table_2.some_field = database_1.table_1.some_other_field)
        WHERE database_1.table_1.data_1 LIKE database_2.table_2.data_2;"""

    parser = SQLParser(sql_query)
    actual = parser.get_tables()
    expected = [('table_1', None), ('table_2', None)]

    assert set(actual) == set(expected)


def test_get_single_field():
    sql_query = 'select title from post;'

    parser = SQLParser(sql_query)
    actual = parser.get_fields()
    expected = ['title']

    assert actual == expected


def test_get_multiple_fields_with_alias():

    sql_query = """SELECT child_entry,asdf AS inode, creation
              FROM links
              WHERE parent_dir == :parent_dir AND name == :name
              LIMIT 1"""

    parser = SQLParser(sql_query)
    actual = parser.get_fields()
    expected = ['child_entry', 'asdf', 'creation']

    assert set(actual) == set(expected)


def test_get_multiple_fields_from_join_with_alias():
    sql_query = """SELECT e.last_name,
        e.department_id,
        d.department_name
        FROM   employees e
        LEFT OUTER JOIN department d
            ON ( e.department_id = d.department_id ); """

    parser = SQLParser(sql_query)
    actual = parser.get_fields()
    expected = ['last_name', 'department_id', 'department_name']

    assert set(actual) == set(expected)

#
# def test_get_fields_by_table():
#     sql_query = """SELECT e.last_name,
#         e.department_id,
#         d.department_name
#         FROM   employees e
#         LEFT OUTER JOIN department d
#             ON ( e.department_id = d.department_id ); """
#
#     parser = SQLParser(sql_query)
#     actual = parser.get_fields_from('employees')
#     expected = ['last_name', 'department_name']
#
#     assert set(actual) == set(expected)
#
