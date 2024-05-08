import requests
from config.etl import etl_config


class EtlConfigError(Exception):
    """
    Generic error for etl config
    """

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "{0} ".format(self.message)
        else:
            return "EtlConfigError"


class ConfigFromDict:
    def __init__(self, cfg_dict):
        if not isinstance(cfg_dict, dict):
            raise TypeError(
                f"Config must be dict; found type {type(cfg_dict).__name__}"
            )

        self._config_dict = cfg_dict
        for key, value in self._config_dict.items():
            setattr(self, key, value)


class EtlConfig(ConfigFromDict):
    def __init__(self, cfg_dict: dict):
        super().__init__(cfg_dict)
        self.workspace_path = self.get_workspace_path()

    def get_workspace_path(self):
        return f"/Workspace/{self.workspace_directory}"


def get_config():
    return EtlConfig(etl_config)