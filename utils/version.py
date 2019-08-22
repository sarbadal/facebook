_API_VERSION = {
    'version': 'v3.3'
}

class APIVersion(object):
    """
    This is just a simple way to keep the facebook ad api version.
    """
    def __init__(self, version=_API_VERSION['version']):
        self.__version = version

    @property
    def version(self):
        return self.__version

    @version.setter
    def version(self, version=_API_VERSION['version']):
        self.__version = version


if __name__ == '__main__':
    v = APIVersion().version
    print(v)
