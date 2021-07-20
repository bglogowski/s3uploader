
class LocalFile(object):
    """

    """
    def __init__(self, name: str, path: str, base_path: str):

        # Use setters to validate input
        self.base_path = base_path
        self.name = name
        self.path = path
        self.s3key = name
        self.uploadable = False

        # Allow lazy loading of these values
        self._size = None
        self._hash = None
        self._metadata = None

    @classmethod
    def from_csv(cls, text: str):
        name, path, base_path = [v.strip() for v in text.split(',')]
        return cls(name, path, base_path)

    @staticmethod
    def exists(path: str) -> bool:
        return os.path.isfile(path)

    @staticmethod
    def sha256(path: str) -> str:

        file_buffer: int = 65536
        sha256 = hashlib.sha256()

        with open(path, 'rb') as f:
            while True:
                data = f.read(file_buffer)
                if not data:
                    break
                sha256.update(data)

        logging.debug(str(__class__.__name__) + ".sha256 = " + sha256.hexdigest())
        return sha256.hexdigest()

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value):
        self._base_path = value
        logging.debug(str(self.__class__.__name__) + ".base_path = " + self._base_path)

    @property
    def full_path(self) -> str:
        return self._full_path

    @property
    def hash(self) -> str:
        if self._hash is None:
            self._hash = self.sha256(self.full_path)
            logging.debug(str(self.__class__.__name__) + ".hash = " + self._hash)
        return self._hash

    @property
    def metadata(self) -> dict:
        if self._metadata is None:
            self._metadata = {"Metadata": {"sha256": self.hash}}
        return self._metadata

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        logging.debug(str(self.__class__.__name__) + ".name = " + self._name)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value
        logging.debug(str(self.__class__.__name__) + ".path = " + self._path)

        self._full_path = self.path + "/" + self.name
        logging.debug(str(self.__class__.__name__) + ".full_path = " + self._full_path)

        relative_path = self.full_path.replace(self.base_path, "")
        relative_path = re.sub(r'^/', '', relative_path)
        relative_path = re.sub(r'^\./', '', relative_path)
        self._relative_path = relative_path
        logging.debug(str(self.__class__.__name__) + ".relative_path = " + self._relative_path)

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def s3key(self):
        return self._s3key

    @s3key.setter
    def s3key(self, value):
        self._s3key = value
        logging.debug(str(self.__class__.__name__) + ".s3key = " + self._s3key)

    @property
    def size(self) -> float:
        if self._size is None:
            self._size = float(os.path.getsize(self.full_path))
            logging.debug(str(self.__class__.__name__) + ".size = " + str(self._size))
        return self._size

    @property
    def uploadable(self):
        return self._uploadable

    @uploadable.setter
    def uploadable(self, value):
        if type(value) is bool:
            self._uploadable = value
        else:
            logging.error("Trying to set variable to non-boolean type. Exiting...")
            sys.exit(2)
        logging.debug(str(self.__class__.__name__) + ".uploadable = " + str(self._uploadable))
