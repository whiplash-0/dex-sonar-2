from configparser import RawConfigParser
from datetime import timedelta
from os import getcwd, path
from zoneinfo import ZoneInfo


CONFIGS_DIR = 'configs'


class Config(RawConfigParser):
    def read(self, file_name, directory_path=CONFIGS_DIR, **kwargs):
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

    def get_timezone(self, section, option, default: ZoneInfo = None, **kwargs) -> ZoneInfo | None:
        return ZoneInfo(self.get(section, option, **kwargs)) if self.get(section, option, **kwargs) else default


config = Config()

config.read('config.ini')
config.read('dev.ini')

TEST_MODE = config.getboolean('Bot', 'test mode')
if TEST_MODE: config.read('test.ini')
