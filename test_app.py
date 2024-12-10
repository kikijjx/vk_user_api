import pytest
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

@pytest.fixture
def create_test_user():
    test_user_data = {
        "id": 12345,
        "label": "User",
        "name": "Djon Djon",
        "sex": 1,
        "city": "abudhabi",
        "screen_name": "djonsdjons",
        "follows": [],
        "subscribed": []
    }
    headers = {"Authorization": "Bearer tokenchik"}
    response = client.post("/nodes", json=test_user_data, headers=headers)
    assert response.status_code == 200
    return test_user_data

def test_get_user(create_test_user):
    headers = {"Authorization": "Bearer tokenchik"}
    response = client.get("/user/12345", headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "id": 12345,
        "name": "Djon Djon",
        "screen_name": "djonsdjons",
        "sex": 1,
        "city": "abudhabi"
    }

def test_get_top_users():
    response = client.get("/top-users")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_top_groups():
    response = client.get("/top-groups")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_users_count():
    response = client.get("/users-count")
    assert response.status_code == 200
    assert "users_count" in response.json()

def test_get_groups_count():
    response = client.get("/groups-count")
    assert response.status_code == 200
    assert "groups_count" in response.json()

def test_get_all_nodes():
    response = client.get("/nodes")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_delete_node_and_relations(create_test_user):
    headers = {"Authorization": "Bearer tokenchik"}

    response = client.get("/user/12345", headers=headers)
    assert response.status_code == 200

    response = client.delete("/nodes/User/12345", headers=headers)
    assert response.status_code == 200
    assert response.json() == {"status": "success"}

    response = client.get("/user/12345", headers=headers)
    assert response.status_code == 404
