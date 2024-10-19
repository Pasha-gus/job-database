import requests

from src.base_api import BaseAPI


class HeadHunterAPI(BaseAPI):
    def __init__(self):
        self.__url = ""
        self.__connections = False

    def get_status(self) -> bool:
        try:
            response = requests.get("https://api.hh.ru/vacancies")
            self.__connections = response.status_code == 200
        except requests.RequestException:
            self.__connections = False
        return self.__connections

    def get_vacancies(self, query: str, per_page: int) -> list:
        if self.get_status():
            self.__url = f"https://api.hh.ru/vacancies?text={query}&per_page={per_page}&employer_id=1740&employer_id=4219&employer_id=566&employer_id=2381&employer_id=2748&employer_id=1057&employer_id=1942330&employer_id=3776&employer_id=4670572&employer_id=115&employer_id=5744484"
            try:
                response = requests.get(self.__url)
                response.raise_for_status()
                return response.json().get("items", [])
            except (requests.RequestException, ValueError) as e:
                print(f"Ошибка при получении вакансий: {e}")
                return []
        else:
            print("Нет подключения к API")
            return []
