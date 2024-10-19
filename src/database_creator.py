import psycopg2
from psycopg2 import sql


def create_database(database_name: str, params: dict):
    """Создает базу данных. Принимает на вход имя базы данных в формате str и словарь с данными для подключения к БД."""

    try:
        # Создаем объект соединения для подключения к существующей базе данных 'postgres'
        conn = psycopg2.connect(dbname="postgres", **params)
        # Устанавливаем режим autocommit
        conn.autocommit = True

        with conn.cursor() as cur:
            # Удаляем базу данных, если она существует
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name)))
            # Создаем новую базу данных
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
            print(f"База данных '{database_name}' успешно создана.")

    except psycopg2.Error as e:
        print(f"Произошла ошибка при работе с PostgreSQL: {e}")

    finally:
        if conn:
            conn.close()


def create_tables(db_name: str, params: dict):
    """Создает таблицу в базе данных"""

    try:
        # Создаем обьект для подключения к существующей базе данных в базе данных 'postgreas'
        conn = psycopg2.connect(dbname=db_name, **params)
        with conn:
            with conn.cursor() as cur:
                # Создаем таблицу
                cur.execute("DROP TABLE IF EXISTS employer")
                cur.execute(
                    """CREATE TABLE employer(
                        employer_id INT PRIMARY KEY,
                        employer_name VARCHAR(50) NOT NULL,
                        employer_url TEXT
                        )"""
                )
                print("Таблица 'employer' успешно создана")
                # Создаем таблицу vacancy
                cur.execute("DROP TABLE IF EXISTS vacancy")
                cur.execute(
                    """CREATE TABLE vacancy(
                        vacancy_id INT PRIMARY KEY,
                        employer_id INT NOT NULL,
                        vacancy_name VARCHAR(100) NOT NULL,
                        salary_from NUMERIC(10, 2),
                        salary_to NUMERIC(10,2),
                        currency VARCHAR(3),
                        requirement TEXT,
                        responsibility TEXT,
                        vacancy_url TEXT NOT NULL,
                        CONSTRAINT pk_vacancy_employer_id FOREIGN KEY(employer_id) REFERENCES employer(employer_id)
                        )"""
                )
                print("Таблица 'vacancy' успешно создана")

    except Exception as ex:
        print(ex)


def insert_in_tables(db_name: str, params: str, data: list):
    """Заполняет таблицы данными из запроса."""
    try:
        with psycopg2.connect(dbname=db_name, **params) as conn:
            with conn.cursor() as cur:
                for vacancy in data:
                    employer = vacancy["employer"]
                    # Вставка данных о работодателе с обработкой дубликатов
                    cur.execute(
                        """
                        INSERT INTO employer (employer_id, employer_name, employer_url) VALUES (%s, %s, %s)
                        ON CONFLICT (employer_id) DO NOTHING;
                    """,
                        (employer["id"], employer["name"], employer["url"]),
                    )

                for vacancy in data:
                    id = vacancy.get("id")
                    employer_id = vacancy["employer"]["id"]
                    name = vacancy.get("name")
                    url = vacancy.get("alternate_url")

                    salary_info = vacancy.get("salary") if vacancy.get("salary") else {}
                    salary_from = salary_info.get("from")
                    salary_to = salary_info.get("to")
                    currency = salary_info.get("currency", "")

                    snippet = vacancy.get("snippet") if vacancy.get("snippet") else {}
                    requirement = snippet.get("requirement", "")
                    responsibility = snippet.get("responsibility", "")

                    # Вставка данных о вакансии с обработкой дубликатов
                    cur.execute(
                        """
                        INSERT INTO vacancy (vacancy_id, employer_id, vacancy_name, salary_from, salary_to, currency, requirement, responsibility, vacancy_url) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (vacancy_id) DO NOTHING;
                    """,
                        (id, employer_id, name, salary_from, salary_to, currency, requirement, responsibility, url),
                    )

                print("Таблицы успешно заполнены.")
    except Exception as ex:
        print("Произошла ошибка:", ex)
