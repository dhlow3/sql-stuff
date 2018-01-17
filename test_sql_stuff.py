# -*- coding: utf-8 -*-
"""Tests for sql_stuff.py module."""
import unittest
import sql_stuff
import sqlalchemy

from os.path import dirname, join
from pandas.core.frame import DataFrame

SERVER = ''  # A valid server to use for testing
DATABASE = ''  # A valid database to use for testing
FILE_Q = join(dirname(__file__),
              'test_sql_file.sql')  # Path to valid sql file returning results


class TestSQLConnection(unittest.TestCase):
    """Tests for SQLConnection class."""

    def setUp(self):
        unittest.TestCase.setUp(self)

        self.sqlconnection = sql_stuff.SQLConnection(
            server=SERVER, database=DATABASE)

    def test_init(self):
        """Test init of SQLConnection.

        Ensure a sqlalchemy engine and metadata are created on init."""

        self.assertIsInstance(self.sqlconnection.engine,
                              sqlalchemy.engine.base.Engine)

        self.assertIsInstance(self.sqlconnection.meta,
                              sqlalchemy.sql.schema.MetaData)

    def test_str_query(self):
        """Test execution of a query from a str."""

        str_q = "SELECT 'a' [ColumnA]"

        results = self.sqlconnection.execute_sql(str_q, return_results=True)
        self.assertIsInstance(results, DataFrame)

    def test_file_query(self):
        """Test the execution of a query from a file."""

        file_q = FILE_Q

        results = self.sqlconnection.execute_sql(file_q, return_results=True)
        self.assertIsInstance(results, DataFrame)

    def test_divide_by_exception(self):
        """Test exception catching in query."""

        divide_by_q = "SELECT 1/0"

        with self.assertRaises(sqlalchemy.exc.DataError):
            self.sqlconnection.execute_sql(divide_by_q)

    def test_sql_comments(self):
        "Test use of /* */ style comments instead of -- style comments."

        comment_q = "SELECT --ABS"

        with self.assertRaises(AssertionError):
            self.sqlconnection.execute_sql(comment_q)


if __name__ == '__main__':
    unittest.main()
