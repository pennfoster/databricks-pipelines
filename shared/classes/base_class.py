from shared.functions.azure_utilities import get_mount_paths


class BaseClass:
    def __init__(self, **kwargs):
        self.data_source = kwargs.get("data_source")
        if self.data_source:
            self._set_file_paths()

    def _set_file_paths(self):
        self.storage_paths = get_mount_paths(self.data_source)
