#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from sqlparse.sql import IdentifierList, Identifier
from sqlparse import parse
from sqlparse.tokens import (Comment, Keyword, Name, DML)

import click

import pyorient
from pyorient.exceptions import (
    PyOrientConnectionException,
    PyOrientSecurityAccessException,
    PyOrientSchemaException
)

from pathlib import Path


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
                yield (identifier.get_real_name(), identifier.get_alias())
        elif isinstance(item, Identifier):
            yield (item.get_real_name(), item.get_alias())
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


def extract_tokens(token_stream):

    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier
        else:
            yield item


def extract_field_identifiers(token_stream, table_name=None, tables=None):
    mode = 0
    oldValue = ""

    table = [t for t in tables if t[0] == table_name]
    for item in token_stream:
        token_type = item.ttype
        value = item.value
        if token_type in Comment:
            continue

        # We have not detected a SELECT statement
        if mode == 0:
            if token_type in Keyword and value.upper() == 'SELECT':
                mode = 1

        # We have detected a SELECT statement
        elif mode == 1:

            if value.upper() == 'FROM':
                mode = 3    # Columns have been checked

            elif value.upper() == 'AS':
                mode = 2

            elif isinstance(item, Identifier):
                if(table):
                    name, alias = table[0]
                    if(item.get_parent_name() == name or item.get_parent_name() == alias):
                        yield item.get_real_name()
                        oldValue = item.get_real_name()

                else:
                    yield item.get_name() if not item.has_alias() else item.get_real_name()
                    oldValue = item.get_real_name()

        # We are processing an AS keyword
        elif mode == 2:
            # We check also for Keywords because a bug in SQLParse
            if token_type == Name or token_type == Keyword:
                yield oldValue
                mode = 1


class SQLParser():
    def __init__(self, sql_query):
        self.sql_query = sql_query

    def get_databases(self):
        stream = extract_from_part(parse(self.sql_query)[0])
        databases = list(extract_database_identifiers(stream))
        return [d for d in databases if d is not None]

    def get_tables(self):
        stream = extract_from_part(parse(self.sql_query)[0])
        tables = list(extract_table_identifiers(stream))
        return [t for t in tables if t is not None]

    def get_fields(self, table_name=None):
        stream = extract_tokens(parse(self.sql_query)[0].tokens)
        tables = self.get_tables()
        columns = extract_field_identifiers(stream, table_name, tables)
        return list(columns)

    def get_fields_from(self, table_name):
        return self.get_fields(table_name)


class GraphDB():

    def __init__(self, host='127.0.0.1', port=2424, user=None, password=None, database='sql2graph'):
        self.user = user
        self.password = password

        self.client = pyorient.OrientDB(host, port)
        self.session_id = self.client.connect(user, password)
        self.database_name = database

    def db_exists(self):
        return self.client.db_exists(self.database_name)

    def db_create(self):
        self.client.db_create(self.database_name, pyorient.DB_TYPE_GRAPH)

    def db_create_class(self, class_name, class_super='V'):
        try:
            self.client.command('create class {0} extends {1}'.format(class_name, class_super))
        except PyOrientSchemaException:
            pass  # :

    def initialize(self):
        """Create Vertex and Edges if not exists
            File has_query Query
            Query uses_database Database
            Query uses_table Table
            Query uses_field Field
            Table has_field Field
            Database has_table Table
        """

        cluster_info = self.client.db_open(self.database_name, self.user, self.password)
        # TODO: cluste_info tiene dataCluster para saber si las bases ya existen
        vertex = [
            'File',
            'Database',
            'Query',
            'Table',
            'Field'
        ]
        for v in vertex:
            self.db_create_class(v)

        print('Vertex {}'.format(vertex))
        edges = [
            'has_table',
            'has_query',
            'uses_database',
            'uses_field',
            'uses_table',
            'has_field '
        ]
        for e in edges:
            self.db_create_class(e, 'E')

        print('Edges {}'.format(edges))

    def save(self, graph_objects):
        for graph_object in graph_objects:
            path = graph_object.get('path')
            self.client.db_open(self.database_name, self.user, self.password)
            file_records = self.client.command('insert into File set path ="{0}"'.format(path))
            # print(file_records[0]._rid)
            for query in graph_object.get('querys'):
                if query.get('query'):
                    query_records = self.client.command('insert into Query set statement = "{0}"'.format(query.get('query')))
                    self.client.command('create edge has_query from {0} to {1}'.format(file_records[0]._rid, query_records[0]._rid))

                    database_records = []
                    for database in query.get('databases'):
                        database_records = self.client.command('insert into Database set name = "{0}"'.format(database))
                        self.client.command('create edge uses_database from {0} to {1}'.format(query_records[0]._rid, database_records[0]._rid))

                    for table in query.get('tables_with_fields'):
                        table_records = self.client.command('insert into Table set name = "{0}"'.format(table[0]))
                        self.client.command('create edge uses_table from {0} to {1}'.format(query_records[0]._rid, table_records[0]._rid))
                        if(database_records):
                            self.client.command('create edge has_table from {0} to {1}'.format(database_records[0]._rid, table_records[0]._rid))

                        fields = query.get('tables_with_fields')[table]
                        for field in fields:
                            field_records = self.client.command('insert into Field set name = "{0}"'.format(field))
                            self.client.command('create edge has_field from {0} to {1}'.format(table_records[0]._rid, field_records[0]._rid))
                            self.client.command('create edge uses_field from {0} to {1}'.format(query_records[0]._rid, field_records[0]._rid))



@click.command()
@click.option('--host', default='127.0.0.1', help='OrientDB server')
@click.option('--port', default=2424, help='Puerto para conectarse a OrientDB')
@click.option('--user', help='Nombre de usuario de acceso a OrientDB', required=True)
@click.option('--password', prompt='Contraseña OrientDB', hide_input=True)
@click.option('--database', default='sql2graph', help='Nombre de base de datos en orientdb')
@click.option('--path', default='.', help='Carpeta con archivos sql')
def init(host='127.0.0.1', port=2424, user=None, password=None, database='sql2graph', path='.'):
    click.echo('Contectando a %s@%s/%s' % (user, host, database))

    try:
        graphdb = GraphDB(host=host, port=port, user=user, password=password, database=database)
        if not graphdb.db_exists():
            click.echo('No existe base de datos %s ' % database)
            if(click.confirm(u'¿Deseas crearla?')):
                click.echo('Creando...')
                graphdb.db_create()
            else:
                click.echo('Bye.')
                return
        # Base creada comenzar a procesar los querys
        click.echo('Procesando...')
        graphdb.initialize()

        click.echo('Leyendo archivos: ')
        p = Path(path)

        graph_objects = []
        for f in p.glob('*.sql'):
            graph_object = {'path': str(f.resolve())}
            with f.open() as sqlFile:
                # WARNING: Assume that files only have selects in one line
                # and is a valid sql expression
                graph_object['querys'] = []
                for selectStatement in sqlFile:
                    parser = SQLParser(selectStatement)
                    databases = parser.get_databases()
                    tables = parser.get_tables()
                    fields = parser.get_fields()
                    tables_with_fields = {}
                    for table in tables:
                        tables_with_fields[table] = parser.get_fields_from(table[0])

                    query = {
                        'query': selectStatement,
                        'databases': databases,
                        'tables': tables,
                        'fields': fields,
                        'tables_with_fields': tables_with_fields
                    }
                    graph_object['querys'].append(query)

            graph_objects.append(graph_object)

        graphdb.save(graph_objects)
        click.echo('Done!')

    except PyOrientConnectionException:
        click.echo('Error al intentar conectarse a OrientDB', err=True)
    except PyOrientSecurityAccessException:
        click.echo(u'No se pudo establecer conexión con los datos proporcionados', err=True)

if __name__ == "__main__":
   init()
