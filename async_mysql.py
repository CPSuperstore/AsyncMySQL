import mysql.connector
import time
import queue
import threading
import sqlescapy
import typing

import AsyncMySQL.query as query


class AsyncMySQL:
    def __init__(self, **kwargs):
        self.connection_info = kwargs
        self.conn = self.connect()

        self.process_thread = None          # type: threading.Thread

        self.command_queue = queue.Queue()

        self.start()

    def start(self, blocking: bool = False):
        if blocking:
            self.process_queries()
        else:
            self.process_thread = threading.Thread(target=self.process_queries)
            self.process_thread.setDaemon(True)
            self.process_thread.setName("AsyncMySQLCommandProcessor")
            self.process_thread.start()

    def connect(self):
        c = 1
        while True:
            try:
                c = mysql.connector.connect(**self.connection_info)
                break

            except mysql.connector.errors.InterfaceError:
                time.sleep(2 ** c)
                c += 1

        c.autocommit = True
        return c

    def get_cursor(self, dictionary: bool = True):
        try:
            return self.conn.cursor(buffered=True, dictionary=dictionary)

        except (mysql.connector.OperationalError, NameError, AttributeError):
            self.conn = self.connect()
            return self.get_cursor(dictionary)

    def execute(self, command: str, block: bool = True):
        command = query.Query(command)

        self.command_queue.put_nowait(command)

        if block:
            while True:
                if command.completed:
                    return command

    def process_queries(self):
        while True:
            command = self.command_queue.get()  # type: query.Query

            c = self.get_cursor()
            c.execute(command.query)

            try:
                command.rs = c.fetchall()
            except mysql.connector.InterfaceError:
                pass

            command.c = c

            command.completed = True

    @staticmethod
    def escape_string(data: typing.Any) -> str:
        if isinstance(data, str):
            return sqlescapy.sqlescape(data)
        return str(data)

    def create_entity(self, table, data: dict):
        stmt = "INSERT INTO `{}` ({}) VALUES ({})".format(
            table, ", ".join("`{}`".format(k) for k in data.keys()),
            ", ".join("'{}'".format(self.escape_string(v)) for v in data.values())
        )

        return self.execute(stmt)

    def select_all_entity(
            self, table: str, cols: list = None, order_by: typing.Union[str, list] = None,
            order_by_ascending: bool = True
    ):
        return self.select_entity(table, cols=cols, order_by=order_by, order_by_ascending=order_by_ascending)

    def select_entity(
            self, table: str, where: dict = None, cols: list = None, order_by: typing.Union[str, list] = None,
            order_by_ascending: bool = True
    ):
        if cols is None:
            cols = "*"
        else:
            cols = ", ".join("`{}`".format(k) for k in cols)

        if order_by is None:
            order_by = ""
        else:
            if isinstance(order_by, str):
                order_by = [order_by]

            order_by = ", ".join(order_by) + " ASC " if order_by_ascending else " DESC "

        search_query = ""
        if where is not None:
            search_query = " WHERE " + " AND ".join(
                "`{}`='{}'".format(k, self.escape_string(v)) for k, v in where.items()
            )

        stmt = "SELECT {} FROM `{}` {} {}".format(
            cols, table, search_query, order_by
        )

        return self.execute(stmt)

    def update_entity(self, table: str, data: dict, where: dict = None):
        search_query = ""
        if where is not None:
            search_query = " WHERE " + " AND ".join(
                "`{}`='{}'".format(k, self.escape_string(v)) for k, v in where.items()
            )

        stmt = "UPDATE `{}` SET {} {}".format(
            table, ", ".join("`{}`='{}'".format(k, self.escape_string(v)) for k, v in data.items()), search_query
        )

        return self.execute(stmt)

    def delete_entity(self, table: str, where: dict = None):
        search_query = ""
        if where is not None:
            search_query = " WHERE " + " AND ".join(
                "`{}`='{}'".format(k, self.escape_string(v)) for k, v in where.items()
            )

        stmt = "DELETE FROM `{}` {}".format(
            table, search_query
        )

        return self.execute(stmt)
