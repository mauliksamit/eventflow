import pytest

from eventflow.config import Config


@pytest.fixture(scope="session")
def spark():
    from eventflow.transform import spark_session
    s = spark_session(Config(lake="data/test-lake", spark_master="local[2]", shuffle_partitions=2), "eventflow-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
