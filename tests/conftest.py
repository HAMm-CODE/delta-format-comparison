"""Shared pytest fixtures."""

import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from deltacompare.session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole suite — JVM startup is slow."""
    session = get_spark("tests", cores=2)
    yield session
    session.stop()


@pytest.fixture
def tmp_data_dir():
    """A clean directory per test, removed afterwards."""
    path = tempfile.mkdtemp(prefix="deltacompare-test-")
    yield path
    shutil.rmtree(path, ignore_errors=True)
