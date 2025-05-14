import sys
from configparser import RawConfigParser
from contextlib import contextmanager
from pathlib import Path
from zoneinfo import ZoneInfo

from src.utils import utils
from src.utils.paths import Paths
from src.utils.time import Timedelta


DEFAULT_PRESET = 'default'
CUSTOM_PRESETS = ['production', 'test']


CONFIG = None


class Config(RawConfigParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.directory_path = None

    @contextmanager
    def within(self, directory_path: Path):
        self.directory_path = directory_path
        yield
        self.directory_path = None
        
    def read(self, name: str, **kwargs):
        super().read(self.directory_path / (name + '.ini'), **kwargs)

    def getint(self, section, option, default: int = None, **kwargs) -> int | None:
        if string := self.get(section, option, **kwargs): return utils.parse_large_number(string, as_type=int)
        else: return default

    def getfloat(self, section, option, default: float = None, **kwargs) -> float | None:
        if string := self.get(section, option, **kwargs): return utils.parse_large_number(string, as_type=float)
        else: return default

    def get_percent(self, section, option, default: float = None, **kwargs) -> float | None:
        """
        Converts a percent to a fraction
        """
        return super().getfloat(section, option, **kwargs) / 100 if super().get(section, option, **kwargs) else default

    def get_timedelta_from_seconds(self, section, option, default: Timedelta = None, **kwargs) -> Timedelta | None:
        return Timedelta(seconds=self.getint(section, option, **kwargs)) if self.get(section, option, **kwargs) else default

    def get_timedelta_from_minutes(self, section, option, default: Timedelta = None, **kwargs) -> Timedelta | None:
        return Timedelta(minutes=self.getint(section, option, **kwargs)) if self.get(section, option, **kwargs) else default

    def get_timedelta_from_hours(self, section, option, default: Timedelta = None, **kwargs) -> Timedelta | None:
        return Timedelta(hours=self.getint(section, option, **kwargs)) if self.get(section, option, **kwargs) else default

    def get_timezone(self, section, option, default: ZoneInfo = None, **kwargs) -> ZoneInfo | None:
        return ZoneInfo(self.get(section, option, **kwargs)) if self.get(section, option, **kwargs) else default


CONFIG = Config()
_presets = [DEFAULT_PRESET]
if len(sys.argv) > 1 and sys.argv[1] in CUSTOM_PRESETS: _presets.append(sys.argv[1])

for preset_path in [Paths.CONFIGS / x for x in _presets]:
    if not preset_path.exists():
        raise ValueError(f"Preset doesn't exist: '{sys.argv[1]}' ({preset_path})")

    with CONFIG.within(preset_path):
        CONFIG.read('config')
        CONFIG.read('dev')
        if CONFIG.getboolean('Bot', 'test mode'): CONFIG.read('test')
