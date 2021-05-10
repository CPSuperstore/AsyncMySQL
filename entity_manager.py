import AsyncMySQL.async_mysql as async_mysql_module
import typing


class EntityManager:
    def __init__(self, async_mysql: async_mysql_module.AsyncMySQL, table: str):
        self.conn = async_mysql
        self.table = table

    def create(self, data: dict):
        return self.conn.create_entity(self.table, data)

    def get_all(self, cols: list = None, order_by: typing.Union[str, list] = None, order_by_ascending: bool = True):
        return self.conn.select_all_entity(
            self.table, cols=cols, order_by=order_by, order_by_ascending=order_by_ascending
        )

    def get(
            self, where: dict = None, cols: list = None, order_by: typing.Union[str, list] = None,
            order_by_ascending: bool = True
    ):
        return self.conn.select_entity(
            self.table, where=where, cols=cols, order_by=order_by, order_by_ascending=order_by_ascending
        )

    def update(self, data: dict, where: dict = None):
        return self.conn.update_entity(self.table, data, where)

    def delete(self, where: dict = None):
        return self.conn.delete_entity(self.table, where)
