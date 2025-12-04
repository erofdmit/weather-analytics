from __future__ import annotations

import pytest
from app.main import create_app
from fastapi import FastAPI


@pytest.fixture
def app() -> FastAPI:
    return create_app()
