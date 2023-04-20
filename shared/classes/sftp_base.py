from paramiko import Transport, SFTPClient

from shared.classes import BaseClass


class SFTPBase(BaseClass):
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        hostkeys: str,
        port: int = 22,
        **kwargs,
    ):
        self.host = host
        self.username = username
        self.password = password
        self.hostkeys = hostkeys
        self.port = port

        super().__init__(**kwargs)

    def connect_to_host(self):
        socket = (self.host, self.port)
        transport = Transport(socket)
        transport.connect(
            hostkey=None,
            username=self.username,
            password=self.password,
        )

        self.connection = SFTPClient.from_transport(transport)

    def __enter__(self):
        self.connect_to_host()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    def extract_file_to_raw(self, host_dir, host_file, local_subdir: str = ""):
        host_file_path = f"{host_dir}/{host_file}"
        local_file_path = f"{self.storage_paths.landing}/{local_subdir+'/' if local_subdir else ''}{host_file}"
        with self.connection.open(host_file_path, "rb") as download:
            with open(f"/dbfs/{local_file_path}", "wb") as load:
                load.write(download.read())

        return local_file_path
