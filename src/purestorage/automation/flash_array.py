import pypureclient
from pypureclient.flasharray import FileSystem


class FlashArray:
    """Class to represent a Pure Storage FlashArray"""

    def __init__(self, api_token, array_host):
        """Initialize the class"""
        self._api_token = api_token
        self._array_host = array_host
        self._client = None

    def authenticate(self):
        """Authenticate to the array"""
        self._client = pypureclient.flasharray.Client(self._array_host, api_token=self._api_token)

    def get_pods(self):
        """Return the array pods"""
        r = self._client.get_pods()
        return r.items

    def get_pod(self, id=None, name=None):
        """Return the array pods by name or id"""
        r = self.get_pods()
        return list(filter(lambda x: (id and x.id == id) or (name and x.name == name), r))

    def get_file_systems(self):
        """Return the array filesystems"""
        r = self._client.get_file_systems()
        return r.items

    def get_file_system(self, id=None, name=None):
        """Return the array filesystems by name or id"""
        r = self.get_file_systems()
        return list(filter(lambda x: (id and x.id == id) or (name and x.name == name), r))

    def create_file_system(self, name):
        """Create a new filesystem"""
        """If the filesystem needs to created in a pod, use the pod prefix in it's name"""
        f = FileSystem(name=name)
        r = self._client.post_file_systems(f)
        return r.items if r.status_code == 200 else None

    def get_managed_directories(self):
        """Return the array managed directories"""
        r = self._client.get_directories()
        return r.items

    def get_managed_directory(self, id=None, name=None):
        """Return the array managed directories by name or id"""
        r = self.get_managed_directories()
        return list(filter(lambda x: (id and x.id == id) or (name and x.name == name), r))

    def get_policies(self):
        """Return the array policies"""
        r = self._client.get_policies()
        return r.items

    def get_policy(self, id=None, name=None):
        """Return the array policies by name or id"""
        r = self.get_policies()
        return list(filter(lambda x: (id and x.id == id) or (name and x.name == name), r))

    def get_directory_exports(self):
        """Return the array directory exports"""
        r = self._client.get_directory_exports()
        return r.items

    def get_directory_export(self, id=None, name=None):
        """Return the array directory exports by name or id"""
        r = self.get_directory_exports()
        return list(filter(lambda x: (id and x.id == id) or (name and x.export_name == name), r))
