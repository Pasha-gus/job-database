import psycopg2


class DBManager:
    """Класс для работы с базой данных"""

    def __init__(self, dbname: str, params: dict):
        self.dbname = dbname
        self.params = params

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        try:
            with psycopg2.connect(dbname=self.dbname, **self.params) as con:
                with con.cursor() as cur:
                    cur.execute(
                        """
                        SELECT employer_name, COUNT(vacancy.vacancy_id) AS count_vacancy 
                        FROM employer 
                        LEFT JOIN vacancy ON employer.employer_id = vacancy.employer_id 
                        GROUP BY employer_name;
                    """
                    )
                    result = cur.fetchall()
            return result

        except Exception as ex:
            print(f"Произошла ошибка: {ex}")

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию"""

        try:
            with psycopg2.connect(dbname=self.dbname, **self.params) as con:
                with con.cursor() as cur:
                    cur.execute(
                        """select employer_name, vacancy_name, salary_from, salary_to, vacancy_url from employer left join vacancy on vacancy.employer_id=employer.employer_id;
                    """
                    )
                    result = cur.fetchall()
            return result
        except Exception as ex:
            print(f"Произошла ошибка {ex}")

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям"""

        try:
            with psycopg2.connect(dbname=self.dbname, **self.params) as con:
                with con.cursor() as cur:
                    cur.execute(
                        """select avg(nullif((salary_from+salary_to)/2, 0)) as averge_salary from vacancy where salary_from is not null and salary_to is not null;"""
                    )
                    result = cur.fetchall()
            return result
        except Exception as ex:
            print(f"Произошла ошибка: {ex}")

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зрплата выше средней по вакансиям"""

        try:
            with psycopg2.connect(dbname=self.dbname, **self.params) as con:
                with con.cursor() as cur:
                    cur.execute(
                        """select * from vacancy where (salary_from + salary_to)/2 > (select avg(nullif((salary_from+salary_to)/2, 0)) from vacancy);"""
                    )
                    result = cur.fetchall()
            return result
        except Exception as ex:
            print(f"Произошла ошибка {ex}")

    def get_vacancies_with_keyword(self, words: list[str]):
        """Получает список всех вакансий в названии которых содержатся переданные в метод слова, например 'python'."""

        try:
            with psycopg2.connect(dbname=self.dbname, **self.params) as con:
                with con.cursor() as cur:
                    # Создание строки запроса с учетом всех слов
                    query = "SELECT * FROM vacancy WHERE " + " OR ".join(["vacancy_name ILIKE %s" for _ in words])
                    cur.execute(query, ["%" + word + "%" for word in words])
                    result = cur.fetchall()
            return result

        except Exception as ex:
            print(f"Произошла ошибка: {ex}")
