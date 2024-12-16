from configparser import ConfigParser
from datetime import timedelta
from os import getcwd, path


class Config(ConfigParser):
    def read(self, file_name, directory_path='configs', **kwargs):
        super().read(path.join(getcwd(), directory_path, file_name))

    def getint(self, section, option, default: int = None, **kwargs) -> int | None:
        return super().getint(section, option, **kwargs) if self.get(section, option, **kwargs) else default

    def getfloat(self, section, option, default: float = None, **kwargs) -> float | None:
        return super().getfloat(section, option, **kwargs) if self.get(section, option, **kwargs) else default

    def get_timedelta_from_seconds(self, section, option, default: timedelta = None, **kwargs) -> timedelta | None:
        return timedelta(seconds=self.getint(section, option, **kwargs)) if self.get(section, option, **kwargs) else default

    def get_timedelta_from_minutes(self, section, option, default: timedelta = None, **kwargs) -> timedelta | None:
        return timedelta(minutes=self.getint(section, option, **kwargs)) if self.get(section, option, **kwargs) else default

    def get_timedelta_from_hours(self, section, option, default: timedelta = None, **kwargs) -> timedelta | None:
        return timedelta(hours=self.getint(section, option, **kwargs)) if self.get(section, option, **kwargs) else default


config = Config()
config.read('config.ini')
config.read('dev.ini')
if config.getboolean('Bot', 'testing_mode'): config.read('testing.ini')
