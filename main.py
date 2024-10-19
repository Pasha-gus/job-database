from src.config import config
from src.database_creator import create_database, create_tables, insert_in_tables
from src.DBManager import DBManager
from src.head_hunter_api import HeadHunterAPI


def main():
    api = HeadHunterAPI()
    query = input("Введите поисковой запрос: ")
    per_page = input("Введите колличество возвращаемых результатов: ")
    vacancies_data = api.get_vacancies(query, per_page)
    print(vacancies_data)
    params = config()
    create_database("Vacancy", params)
    create_tables("Vacancy", params)
    insert_in_tables("Vacancy", params, vacancies_data)
    dbmaneger = DBManager("Vacancy", params)
    print("Список всех компаний и количество вакансий у каждой компании:")
    vacancy_count = dbmaneger.get_companies_and_vacancies_count()
    print(vacancy_count)
    print("Cписок всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию: \n")
    all_vacancy = dbmaneger.get_all_vacancies()
    print(f"{all_vacancy}\n")
    print("Средняя зарплата по вакансиям:")
    avg_salary = dbmaneger.get_avg_salary()
    print(f"{avg_salary}\n")
    print("список всех вакансий, у которых зарплата выше средней по всем вакансиям:")
    higer_salary = dbmaneger.get_vacancies_with_higher_salary()
    print(f"{higer_salary}\n")
    words = input("Введите слова для поиска вакансий: ")
    print(f"Cписок всех вакансий, в названии которых содержатся {words}")
    keyword = dbmaneger.get_vacancies_with_keyword(words.split())
    print(f"{keyword}\n")


if __name__ == "__main__":
    main()
