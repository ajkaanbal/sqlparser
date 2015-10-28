#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlparse.sql import IdentifierList, Identifier
from sqlparse import parse
from sqlparse.tokens import (Comment, Keyword, Name, DML)

import click

import pyorient
from pyorient.exceptions import (
    PyOrientConnectionException,
    PyOrientSecurityAccessException
)


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

        self.client = pyorient.OrientDB(host, port)
        self.session_id = self.client.connect(user, password)
        self.database_name = database

    def db_exists(self):
        return self.client.db_exists(self.database_name)

    def db_create(self):
        self.client.db_create(self.database_name)


@click.command()
@click.option('--host', default='127.0.0.1', help='OrientDB server')
@click.option('--port', default=2424, help='Puerto para conectarse a OrientDB')
@click.option('--user', help='Nombre de usuario de acceso a OrientDB', required=True)
@click.option('--password', prompt='Contraseña OrientDB', hide_input=True)
@click.option('--database', default='sql2graph', help='Nombre de base de datos en orientdb')
def init(host='127.0.0.1', port=2424, user=None, password=None, database='sql2graph'):
    click.echo('Contectando a %s@%s/%s' % (user,host,database))

    try:
        graphdb  = GraphDB(host=host, port=port, user=user, password=password, database=database)
        if not graphdb.db_exists():
            click.echo('No existe base de datos %s ' % database)
            if(click.confirm(u'¿Deseas crearla?')):
                click.echo('Creando...')
                graphdb.db_create();
            else:
                click.echo('Bye.')
                return
        # Base creada comenzar a procesar los querys
        click.echo('Procesando...')

    except PyOrientConnectionException:
        click.echo('Error al intentar conectarse a OrientDB', err=True)
    except PyOrientSecurityAccessException:
        click.echo(u'No se pudo establecer conexión con los datos proporcionados', err=True)

if __name__ == "__main__":
   init()
