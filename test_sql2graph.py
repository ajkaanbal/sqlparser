from sql2graph import SQLParse


def test_get_databases_names_in_simple_query():
    sql_query = 'select title from cms.post;'
    parsed = SQLParse(sql_query)
    assert parsed.uses_database() == ['cms']


def test_get_databases_names_in_complex_query():
    sql_query = """
        SELECT option_value
         FROM `database1`.`wp_options`
         WHERE option_name="active_plugins"
        UNION
        SELECT option_value
         FROM `database2`.`wp_options`
         WHERE option_name="active_plugins"
        """
    parsed = SQLParse(sql_query)
    assert parsed.uses_database() == ['database1', 'database2']
