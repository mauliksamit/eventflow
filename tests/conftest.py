import pytest


@pytest.fixture(scope="session")
def spark():
    from eventflow.transform import spark_session
    s = spark_session("eventflow-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
