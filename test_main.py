from fastapi.testclient import TestClient

from main import app


client = TestClient(app)


def test_create_success_flight():
    with TestClient(app) as _client:
        response = _client.post(
            "/flights/",
            json={
                "flight_id": "A444",
                "arrival": "03:00",
                "departure": "07:00"
            }

        )
        assert response.status_code == 200


def test_get_success_flight():
    response = client.get(
        "/flights/A444"

    )
    assert response.status_code == 200
    assert response.json() == \
           {
               'flights':
                   [
                       {'flight_id': 'A444', 'arrival': '03:00:00', 'departure': '07:00:00', 'success': 'success'}
                   ]
           }


def test_create_fail_flight():
    response = client.post(
        "/flights/",
        json={
            "flight_id": "A444",
            "arrival": "13:00",
            "departure": "15:23"
        }

    )
    assert response.status_code == 200


def test_get_fail_flight():
    response = client.get(
        "/flights/A444"

    )
    assert response.status_code == 200

    print(response.json())

    assert response.json() == \
           {
               'flights':
                   [
                       {'flight_id': 'A444', 'arrival': '13:00:00', 'departure': '15:23:00', 'success': 'fail'}
                   ]
           }


