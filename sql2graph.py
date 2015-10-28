# This example illustrates how to extract table names from nested
# SELECT statements.

# See:
# http://groups.google.com/group/sqlparse/browse_thread/thread/b0bd9a022e9d4895

sql = """
select K.a,K.b from (select H.b from (select G.c from (select F.d from
(select E.e from A, B, C, D, E), F), G), H), I, J, K order by 1,2;
"""

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


def is_subselect(parsed):
    if not parsed.is_group():
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        import pprint; pprint.pprint(item)
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                keywords_whitelist = [
                    'JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN',
                    'FULL OUTER JOIN', 'NATURAL JOIN',
                    'CROSS JOIN', 'STRAIGHT JOIN',
                    'INNER JOIN', 'LEFT INNER JOIN'
                ]
                if item.value.upper() not in keywords_whitelist:
                    raise StopIteration
                else:
                    from_seen = True
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name() if not identifier.has_alias() else identifier.get_real_name()
        elif isinstance(item, Identifier):
            yield item.get_name() if not item.has_alias() else item.get_real_name()
        elif item.ttype is Keyword:
            yield item.value


def extract_database_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_parent_name()
        elif isinstance(item, Identifier):
            yield item.get_parent_name()
        elif item.ttype is Keyword:
            yield item.value


class SQLParser():
    def __init__(self, sql_query):
        self.sql_query = sql_query

    def get_databases(self):
        stream = extract_from_part(sqlparse.parse(self.sql_query)[0])
        databases = list(extract_database_identifiers(stream))
        return [d for d in databases if d is not None]

    def get_tables(self):
        stream = extract_from_part(sqlparse.parse(self.sql_query)[0])
        tables = list(extract_table_identifiers(stream))
        return [t for t in tables if t is not None]
