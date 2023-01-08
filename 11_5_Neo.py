import psycopg2
from pprint import pprint
import pandas as pd


def chek_client_id(conn, client_id):
    cur.execute("""
            select id from Client;
            """)
    chek = cur.fetchall()
    # print(chek)
    # print(type(chek))
    for id in chek:
        # print(id[0])
        if id[0] == client_id:
            return True

    print('Неверно указан client_id')
    return False


def chek_phone_id(conn, phone_id):
    cur.execute("""
            select id from Phone;
            """)
    chek = cur.fetchall()

    for id in chek:
        # print(id[0])
        if id[0] == phone_id:
            return True
    print('Неверно указан phone_id')
    return False


# 1 функция
def create_db(conn):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Client(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email VARCHAR(40) UNIQUE
        );
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Phone(
            id SERIAL PRIMARY KEY,
            phones VARCHAR (10) UNIQUE,
            client_id INTEGER REFERENCES Client(id)
        );
        """)
    conn.commit()

    pprint(pd.read_sql('select * from Client;', conn))
    pprint(pd.read_sql('select * from Phone;', conn))
    return


#2 функция
def add_client(conn, first_name, last_name, email, phone=None):
    # cur.execute(f"INSERT INTO Client(first_name, last_name, email) VALUES('{first_name}', '{last_name}', '{email}');")
    cur.execute("""
        INSERT INTO Client(first_name, last_name, email) VALUES(%s, %s, %s);
        """, (first_name, last_name, email))

    # cur.execute("""
    #     SELECT id FROM Client ORDER BY id DESC LIMIT 1;
    #     """)

    cur.execute("""
        SELECT id FROM Client WHERE email = %s;
        """, (email,))
    # последний id (через уникальный емайл)

    client_id = cur.fetchone()[0]
    # print(client_id)
    # print(type(client_id))

    pprint(pd.read_sql('select * from Client;', conn))

    conn.commit()

    if phone:
        cur.execute("""INSERT INTO Phone (phones, client_id) VALUES(%s, %s);""", (phone, client_id))

    pprint(pd.read_sql('select * from Phone;', conn))

    conn.commit()

    return


#3 функция
def add_phone(conn, client_id, phone):
    cur.execute("""INSERT INTO Phone (phones, client_id) VALUES(%s, %s);""", (phone, client_id))

    pprint(pd.read_sql('select * from Phone;', conn))

    conn.commit()
    return


#4 функция
def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    # обновление данных (U из CRUD)

    if first_name:
        cur.execute("""
           UPDATE Client SET first_name=%s WHERE id=%s;
           """, (first_name, client_id))

    if last_name:
        cur.execute("""
           UPDATE Client SET last_name=%s WHERE id=%s;
           """, (last_name, client_id))

    if email:
        cur.execute("""
           UPDATE Client SET email=%s WHERE id=%s;
           """, (email, client_id))

    pprint(pd.read_sql('select * from Client;', conn))

    conn.commit()

    return

#5 функция
def delete_phone(conn, phone_id):

    cur.execute("""
           DELETE FROM Phone WHERE id=%s;
           """, (phone_id,))
    cur.execute("""
           SELECT * FROM Phone;
           """)
    print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения

    return


#6 функция
def delete_client(conn, client_id):

    cur.execute("""
           DELETE FROM Phone WHERE client_id=%s;
           """, (client_id,))

    cur.execute("""
           DELETE FROM Client WHERE id=%s;
           """, (client_id,))

    cur.execute("""
           SELECT * FROM Client;
           """)
    print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения

    cur.execute("""
           SELECT * FROM Phone;
           """)

    print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения

    return


#7 функция
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):

    if first_name:
        cur.execute("""
            SELECT * FROM Client WHERE first_name=%s;
            """, (first_name,))

        first_ = cur.fetchall()
        print('Клиенты:', first_)

        for id in first_:
            print('Id Клиента:', id[0])
            client_id = id[0]

            cur.execute("""
               SELECT * FROM Phone WHERE client_id=%s;
               """, (client_id,))
            print('Телефоны:', cur.fetchall())
            return

    if last_name:
        cur.execute("""
            SELECT * FROM Client WHERE last_name=%s;
            """, (last_name,))

        last_ = cur.fetchall()
        print('Клиенты:', last_)

        for id in last_:
            print('Id Клиента:', id[0])
            client_id = id[0]

            cur.execute("""
               SELECT * FROM Phone WHERE client_id=%s;
               """, (client_id,))
            print('Телефоны:', cur.fetchall())
            return

    if email:
        cur.execute("""
            SELECT * FROM Client WHERE email=%s;
            """, (email,))

        email_ = cur.fetchall()
        print('Клиенты:', email_)

        for id in email_:
            print('Id Клиента:', id[0])
            client_id = id[0]

            cur.execute("""
               SELECT * FROM Phone WHERE client_id=%s;
               """, (client_id,))
            print('Телефоны:', cur.fetchall())
            return

    if phone:
        cur.execute("""
            SELECT * FROM Phone WHERE phones=%s;
            """, (phone,))

        phone_ = cur.fetchall()
        print('Телефоны:', phone_)

        for id in phone_:
            print('Id Клиента:', id[2])
            client_id = id[2]

            cur.execute("""
               SELECT * FROM Client WHERE id=%s;
               """, (client_id,))
            print('Клиент:', cur.fetchall())
            return

    print('Не найдено!')
    return


# 8
def delete_db(conn):
    cur.execute("""
        DELETE FROM Phone; 
        DELETE FROM Client;     
               """)
    return


with psycopg2.connect(database="test5", user="postgres", password="********") as conn:
    with conn.cursor() as cur:

        while True:
            print()
            print("Меню (нажмите соответствующий номер):")
            print("0. ВЫХОД")
            print("1. Создание структуры БД (таблицы)")
            print("2. Добавить нового клиента")
            print("3. Добавить телефон для существующего клиента")
            print("4. Изменить данные о клиенте")
            print("5. Удалить телефон для существующего клиента")
            print("6. Удалить существующего клиента")
            print("7. Найти клиента по его данным (имени, фамилии, email-у или телефону)")
            print("8. Удалить данные в БД (таблицы)")
            command = int(input())
    #1
            if command == 1:
                create_db(conn)

    #2
            elif command == 2:
                first_name_ = (input("Введите first_name\n"))
                last_name_ = (input("Введите last_name\n"))
                email_ = (input("Введите email\n"))
                phone_ = (input("Введите phones (если нет, то n)\n"))


                if phone_ == 'n':
                    phone_ = None

                add_client(conn, first_name_, last_name_, email_, phone_)


    #3
            elif command == 3:
                pprint(pd.read_sql('select * from Client;', conn))
                pprint(pd.read_sql('select * from Phone;', conn))

                client_id = int(input("Введите client_id\n"))
                phone_ = input("Введите phone\n")
                add_phone(conn, client_id, phone_)

    #4
            elif command == 4:
                pprint(pd.read_sql('select * from Client;', conn))
                pprint(pd.read_sql('select * from Phone;', conn))

                client_id = int(input("Введите client_id\n"))

                if chek_client_id(conn, client_id) == False:
                    continue

                print('Для изменения телефонов используйте 3 или 5 в меню!')
                first_name = input("Введите first_name (если не меняем, то n)\n")
                last_name = input("Введите last_name (если не меняем, то n)\n")
                email = input("Введите email (если не меняем, то n)\n")

                # phone = input("Введите phone (если не добавляем, то n)\n")

                if first_name == 'n':
                    first_name = None

                if last_name == 'n':
                    last_name = None

                if email == 'n':
                    email = None

                change_client(conn, client_id, first_name, last_name, email)

    #5
            elif command == 5:

                pprint(pd.read_sql('select * from Phone;', conn))

                phone_id = int(input("Введите id phone, который нужно удалить\n"))

                if chek_phone_id(conn, phone_id) == False:
                    continue

                delete_phone(conn, phone_id)

    #6
            elif command == 6:
                pprint(pd.read_sql('select * from Client;', conn))

                client_id = int(input("Введите client_id\n"))

                if chek_client_id(conn, client_id) == False:
                    continue

                delete_client(conn, client_id)

    #7
            elif command == 7:
                print("Поиск клиента по:")
                print("1. first_name")
                print("2. last_name")
                print("3. email")
                print("4. phones")
                command_find = int(input())

                pprint(pd.read_sql('select * from Client;', conn))
                pprint(pd.read_sql('select * from Phone;', conn))


                if command_find == 1:
                    first_name = input("Введите first_name\n")
                    find_client(conn, first_name=first_name)

                elif command_find == 2:
                    last_name = input("Введите last_name\n")
                    find_client(conn, last_name=last_name)

                elif command_find == 3:
                    email = input("Введите email\n")
                    find_client(conn, email=email)

                elif command_find == 4:
                    phone = input("Введите phone\n")
                    find_client(conn, phone=phone)

 # 8
            elif command == 8:
                delete_db(conn)


            elif command == 0:
                break

            else:
                print("Повторите команду!\n")

conn.close()
