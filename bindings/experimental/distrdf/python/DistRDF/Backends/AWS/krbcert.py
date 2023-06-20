
class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class KRBCert(metaclass=Singleton):
    TOKEN_PATH = '/tmp/certs'

    def __init__(self):
        self._bytes: bytes = None

    @property
    def bytes(self):
        if self._bytes is None:
            self.get_certs()
        return self._bytes

    def get_certs(self):
        try:
            with open(self.TOKEN_PATH, "rb") as f:
                self._bytes = f.read()
        except FileNotFoundError:
            print("No certificate was found. Make sure to write the appropriate certificate in the path "
                  f"'{self.TOKEN_PATH}'. Disregard this message if you are not reading restricted-access data.")
            self._bytes = b''

