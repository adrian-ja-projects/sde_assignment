#taken example from documentation in fastAPI#
from starlette.testclient import TestClient
from main import app

client = TestClient(app)


def test_read_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"detail": "Healthy"}