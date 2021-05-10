import mysql.connector.cursor


class Query:
    def __init__(self, query: str):
        self.query = query          # type: str
        self.completed = False      # type: bool
        self.rs = []
        self.c = None               # type: mysql.connector.cursor.MySQLCursor

    def __del__(self):
        self.close()

    @property
    def description(self):
        return self.c.description

    @property
    def rowcount(self):
        return self.c.rowcount

    def fetchall(self):
        return self.rs

    def fetchone(self):
        return self.rs[0]

    def fetchmany(self, n=1):
        return self.rs[n:]

    def close(self):
        self.c.close()

    @property
    def lastrowid(self):
        return self.c.lastrowid

    def getlastrowid(self):
        return self.c.getlastrowid()

    @property
    def column_names(self):
        return self.c.column_names

    @property
    def statement(self):
        return self.c.statement

    @property
    def with_rows(self):
        return self.c.with_rows

    def next(self):
        return self.c.__next__()

    def __next__(self):
        return self.c.__next__()

    @property
    def with_rows(self):
        return self.c.with_rows

    def __str__(self):
        return self.c.__str__()
