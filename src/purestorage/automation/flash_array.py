import pypureclient
from pypureclient.flasharray import FileSystem, Directory, Pod, Policy, PolicyRuleNfsClient, PolicyRuleNfsClientPost, \
    PolicyRuleSmbClient, PolicyRuleSmbClientPost, PolicyRuleQuota, PolicyRuleQuotaPost

from purestorage.automation.rest_util import handle_response_with_items, handle_response_with_value, handle_response


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
        return handle_response_with_items(r)

    def get_pod(self, id=None, name=None):
        """Return the array pods by name or id"""
        r = self._client.get_pods(names=[name] if name else None, ids=[id] if id else None)
        return (
            handle_response_with_value(r, list(filter(lambda x: (id and x.id == id)
                                                                or (name and x.name == name), r.items))))

    def create_pod(self, name):
        """Create a new pod"""
        p = Pod(name=name)
        r = self._client.post_pods(pod=p, names=[name])
        return handle_response_with_items(r)

    def destroy_pod(self, name):
        """Destroy a pod"""
        p = Pod(destroyed=True)
        r = self._client.patch_pods(pod=p, destroy_contents=True, names=[name])
        return handle_response_with_items(r)

    def eradicate_pod(self, name):
        """Eradicate a pod"""
        r = self._client.delete_pods(names=[name], eradicate_contents=True)
        return handle_response(r)

    def get_file_systems(self):
        """Return the array filesystems"""
        r = self._client.get_file_systems()
        return handle_response_with_items(r)

    def get_file_system(self, id=None, name=None):
        """Return the array filesystems by name or id"""
        r = self._client.get_file_systems(names=[name] if name else None, ids=[id] if id else None)
        return (
            handle_response_with_value(r, list(filter(lambda x: (id and x.id == id)
                                                                or (name and x.name == name), r.items))))

    def create_file_system(self, name):
        """Create a new filesystem"""
        """If the filesystem needs to created in a pod, use the pod prefix in it's name"""
        f = FileSystem(name=name)
        r = self._client.post_file_systems(f)
        return handle_response_with_items(r)

    def destroy_file_system(self, name):
        """Destroy a file system"""
        f = FileSystem(destroyed=True)
        r = self._client.patch_file_systems(file_system=f, names=[name])
        return handle_response_with_items(r)

    def eradicate_file_system(self, name):
        """Eradicate a file system"""
        r = self._client.delete_file_systems(names=[name])
        return handle_response(r)

    def get_managed_directories(self, file_system_name=None, file_system_id=None):
        """Return the array managed directories"""
        r = self._client.get_directories(file_system_names=[file_system_name] if file_system_name else None,
                                         file_system_ids=[file_system_id] if file_system_id else None)
        return handle_response_with_items(r)

    def get_managed_directory(self, id=None, name=None):
        """Return the array managed directories by name or id"""
        r = self._client.get_directories(names=[name] if name else None, ids=[id] if id else None)
        return (
            handle_response_with_value(r, list(filter(lambda x: (id and x.id == id)
                                                                or (name and x.name == name), r.items))))

    def create_managed_directory(self, name, file_system_name, path=None):
        """Create a new managed directory"""
        """If the managed directory needs to created in a pod, use the pod prefix in it's name"""
        d = Directory(file_system=file_system_name, directory_name=name, path=f'/{name}' if not path else path)
        r = self._client.post_directories(directory=d, file_system_names=[file_system_name])
        return handle_response_with_items(r)

    def delete_managed_directory(self, name):
        """Eradicate a managed directory"""
        r = self._client.delete_directories(names=[name])
        return handle_response(r)

    def get_policy_nfs_rules(self, policy_name=None, policy_id=None):
        """Return the array NFS policy rules"""
        r = self._client.get_policies_nfs_client_rules(policy_names=[policy_name] if policy_name else None,
                                                       policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def create_policy_nfs_rule(self, rule_name=None, client='*', access=None, permission=None, nfs_version=None,
                               security=None, anonuid=None, anongid=None, policy_name=None, policy_id=None):
        """Create a new NFS policy rule"""
        rule = PolicyRuleNfsClient(
            name=rule_name if rule_name else None,
            client=client if client else None,
            access=access if access else None,
            permission=permission if permission else None,
            nfs_version=nfs_version if nfs_version else None,
            security=security if security else None,
            anonuid=anonuid if anonuid else None,
            anongid=anongid if anongid else None)
        r = self._client.post_policies_nfs_client_rules(rules=PolicyRuleNfsClientPost(rules=[rule]),
                                                        policy_names=[policy_name] if policy_name else None,
                                                        policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def get_policy_smb_rules(self, policy_name=None, policy_id=None):
        """Return the array SMB policy rules"""
        r = self._client.get_policies_smb_client_rules(policy_names=[policy_name] if policy_name else None,
                                                       policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def create_policy_smb_rule(self, rule_name=None, client='*', anonymous_access_allowed=None,
                               smb_encryption_required=None, policy_name=None, policy_id=None):
        """Create a new SMB policy rule"""
        rule = PolicyRuleSmbClient(
            name=rule_name if rule_name else None,
            client=client if client else None,
            anonymous_access_allowed=anonymous_access_allowed if anonymous_access_allowed else None,
            smb_encryption_required=smb_encryption_required if smb_encryption_required else None)
        r = self._client.post_policies_smb_client_rules(rules=PolicyRuleSmbClientPost(rules=[rule]),
                                                        policy_names=[policy_name] if policy_name else None,
                                                        policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def get_policy_quota_rules(self, policy_name=None, policy_id=None):
        """Return the array quota policy rules"""
        r = self._client.get_policies_quota_rules(policy_names=[policy_name] if policy_name else None,
                                                  policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def create_policy_quota_rule(self, quota_limit, enforced, rule_name=None,
                                 notifications=None, policy_name=None, policy_id=None):
        """Create a new quota policy rule"""
        rule = PolicyRuleQuota(
            name=rule_name if rule_name else None,
            enforced=enforced if enforced else None,
            quota_limit=quota_limit if quota_limit else None,
            notifications=notifications if notifications else None)

        r = self._client.post_policies_quota_rules(rules=PolicyRuleQuotaPost(rules=[rule]),
                                                          policy_names=[policy_name] if policy_name else None,
                                                          policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def get_policy_snapshot_rules(self, policy_name=None, policy_id=None):
        """Return the array snapshot policy rules"""
        r = self._client.get_policies_snapshot_rules(policy_names=[policy_name] if policy_name else None,
                                                     policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def get_policies(self):
        """Return the array policies"""
        r = self._client.get_policies()
        return handle_response_with_items(r)

    def get_policy(self, id=None, name=None):
        """Return the array policies by name or id"""
        r = self._client.get_policies(names=[name] if name else None, ids=[id] if id else None)
        return (
            handle_response_with_value(r, list(filter(lambda x: (id and x.id == id)
                                                                or (name and x.name == name), r.items))))

    def create_policy_nfs(self, name, enabled=True):
        """Create a new NFS policy"""
        p = Policy(name=name, policy_type='nfs', enabled=enabled)
        r = self._client.post_policies_nfs(policy=p, names=[name])
        return handle_response_with_items(r)

    def create_policy_smb(self, name, enabled=True):
        """Create a new SMB policy"""
        p = Policy(name=name, policy_type='smb', enabled=enabled)
        r = self._client.post_policies_smb(policy=p, names=[name])
        return handle_response_with_items(r)

    def create_policy_quota(self, name, enabled=True):
        """Create a new quota policy"""
        p = Policy(name=name, policy_type='quota', enabled=enabled)
        r = self._client.post_policies_quota(policy=p, names=[name])
        return handle_response_with_items(r)

    def create_policy_snapshot(self, name, enabled=True):
        """Create a new snapshot policy"""
        p = Policy(name=name, policy_type='snapshot', enabled=enabled)
        r = self._client.post_policies_snapshot(policy=p, names=[name])
        return handle_response_with_items(r)

    def get_directory_exports(self, directory_name=None, directory_id=None, policy_name=None, policy_id=None):
        """Return the array directory exports"""
        r = self._client.get_directory_exports(directory_names=[directory_name] if directory_name else None,
                                               directory_ids=[directory_id] if directory_id else None,
                                               policy_names=[policy_name] if policy_name else None,
                                               policy_ids=[policy_id] if policy_id else None)
        return handle_response_with_items(r)

    def get_directory_export(self, id=None, name=None):
        """Return the array directory exports by name or id"""
        r = self._client.get_directory_exports(export_names=[name] if name else None)
        return (
            handle_response_with_value(r, list(filter(lambda x: (id and x.id == id)
                                                                or (name and x.export_name == name), r.items))))
