from functools import wraps
import psycopg2

config_params = {'host': '127.0.0.1',
                 'user': 'postgres',
                 'password': '1234',
                 'database': 'sheets_db'
                 }


def conn_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = psycopg2.connect(**config_params)
        try:
            f = func(conn, *args, **kwargs)
            conn.commit()
            return f
        except psycopg2.Error as e:
            conn.rollback()
            print(e)
        finally:
            conn.close()

    return wrapper


@conn_decorator
def create_data(conn, data):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS test_sheets(
                                № INTEGER,
                                order_num VARCHAR(20),
                                price_$ INTEGER,
                                delivery_time VARCHAR,
                                price_in_rub DECIMAL(8, 2))'''
                   )

    cursor.executemany('''INSERT INTO test_sheets VALUES(%s, %s, %s, %s, %s)''', data)


@conn_decorator
def read_data(conn):
    cursor = conn.cursor()
    cursor.execute('''SELECT CAST(№ AS VARCHAR),
                             order_num,
                             CAST(price_$ AS VARCHAR),
                             delivery_time,
                             CAST(price_in_rub AS VARCHAR)
                          FROM test_sheets''')
    return tuple(cursor.fetchall())


@conn_decorator
def update_data(conn, source_data, new_data):
    cursor = conn.cursor()
    source_data = sorted(source_data, key=lambda x: int(x[0]))
    new_data = sorted(source_data, key=lambda x: int(x[0]))
    for el1, el2 in zip(new_data, source_data):
        cursor.execute(""" UPDATE test_sheets
                           SET № = %s, order_num = %s, price_$ = %s, delivery_time = %s, price_in_rub = %s
                           WHERE № = %s and order_num = %s """, (*el1, *el2[:2]))


@conn_decorator
def delete_data(conn, data):
    cursor = conn.cursor()
    for el in data:
        cursor.execute('''DELETE FROM test_sheets
                              WHERE № = %s and order_num = %s''', el[:2])



