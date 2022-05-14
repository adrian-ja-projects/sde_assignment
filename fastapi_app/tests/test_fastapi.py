#taken example from documentation in fastAPI
from starlette.testclient import TestClient
import pytest

from main import app
#import pytest

client = TestClient(app)


def test_read_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"detail": "Healthy"}