# -*- coding: utf-8 -*-
"""A simple class to connect to a MSSQL database engine and execute queries."""
from os.path import exists
from pandas import read_sql_query
from sqlalchemy import create_engine, MetaData


class SQLConnection:
    """A class for connecting to a database and preforming SQL commands."""

    def __init__(self, server, database):
        """Initialize a sql connection and connect to it.

        Parameters
        ----------
        server: str
            A server.
        database: str
            A database.
        """
        self.server = server
        self.db = database
        self._connect_to_sqlserver()

    def _connect_to_sqlserver(self):
        """Build a Microsoft SQL Server connection.

        Creates
        ----------
        self.engine : sqlalchemy.engine.base.Engine
            The database connection engine.
        self.meta : sqlalchemy.sql.schema.MetaData
            The current meta data for the connection engine.
        """
        driver = 'SQL+Server+Native+Client+11.0'
        self.engine = create_engine('mssql://{}/{}?driver={}'.format(
            self.server, self.db, driver
            ), echo=False, legacy_schema_aliasing=False)
        self.meta = MetaData(self.engine)

    def execute_sql(self, query_input, return_results=False):
        """Execute sql.

        Parameters
        ----------
        query_input: str
            Sql_file path or query.
        results: boolean
            Whether to return results or not (default False).

        Returns
        ----------
        df: pandas.core.frame.DataFrame (default: do not return results)
            The results of the query.
        """
        # If query_input is an existing file, pull query from file
        if exists(query_input):
            # if file is a valid path to a file
            query_file = query_input

            # Get query
            with open(query_file) as f:
                query = f.read()

            # Remove unnecessary characters in query
            query = query.replace('\n', ' ').replace('\t', ' ')

        # If query_input is not file, query_input should be a string query
        else:
            query = query_input

        # Ensure there are no -- style comments in the query
        assert '--' not in query, ('unable to parse queries containing -- '
                                   'style comments, use /* */ instead')

        # Open database connection
        connection = self.engine.connect()
        transaction = connection.begin()

        try:
            # Execute query
            connection.execute(query)
            transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            # Close database connection
            connection.close()

        if return_results:
            results = read_sql_query(query, con=self.engine)
            return results
