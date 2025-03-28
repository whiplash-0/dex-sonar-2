from os import getcwd
from os.path import join

from src.config import parameters


Path = str
FileName = str
Component = str


def get_path(*components: Component) -> Path:
    return join(getcwd(), *components)


def get_config_path(file_name: FileName) -> Path:
    return join(getcwd(), parameters.CONFIGS_DIR, file_name)
