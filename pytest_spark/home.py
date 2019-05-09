import os


class SparkHome(object):

    def __init__(self, pytest_config):
        self.config = pytest_config

        self._path, source_name = self._detect_path()
        if self._path:
            self._path = os.path.abspath(self._path)
            if not os.path.exists(self._path):
                raise OSError(
                    "SPARK_HOME path specified in %s does not exist: %s"
                    % (source_name, self._path))

    @property
    def path(self):
        return self._path

    @property
    def version(self):
        if self.path:
            return self._get_spark_version(self.path)

    def _get_spark_version(self, spark_home):
        release_info_filename = os.path.join(spark_home, 'RELEASE')
        if os.path.exists(release_info_filename):
            with open(release_info_filename) as release_info:
                return release_info.read()

    def _locations(self):
        yield (
            self.config.option.spark_home,
            'config (command line option "--spark_home")',
        )
        yield (self.config.getini('spark_home'), 'config (pytest.ini)')
        yield (os.environ.get('SPARK_HOME'), 'ENV')

    def _detect_path(self):
        for path, description in self._locations():
            if path:
                return path, description
        return None, None
