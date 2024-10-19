from unittest.mock import Mock, patch

from src.head_hunter_api import HeadHunterAPI


def test_get_status_success():
    api = HeadHunterAPI()

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        assert api.get_status() is True


def test_get_vacancies_success():
    api = HeadHunterAPI()

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"items": [{"id": 1, "name": "Vacancy 1"}]}
        mock_get.return_value = mock_response

        vacancies = api.get_vacancies("developer", 5)
        assert len(vacancies) == 1
        assert vacancies[0]["name"] == "Vacancy 1"


def test_get_vacancies_api_failure():
    api = HeadHunterAPI()

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        vacancies = api.get_vacancies("developer", 5)
        assert len(vacancies) == 0
