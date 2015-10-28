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
        WHERE database_1.table_1.data_1 LIKE database_2.table_2.data_2"""

    parser = SQLParser(sql_query)
    actual = parser.get_databases()
    expected = ['database_2', 'database_1']

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
