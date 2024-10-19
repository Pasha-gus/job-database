from abc import ABC, abstractmethod


class BaseAPI(ABC):
    """Абстрактный класс для рфботы с API поиском вакансий"""

    @abstractmethod
    def get_status(self):
        pass

    @abstractmethod
    def get_vacancies(self):
        pass
