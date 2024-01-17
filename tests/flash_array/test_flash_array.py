import os

import pytest

from purestorage.automation import (FlashArray, NfsPolicyRuleVersion, NfsPolicyRuleAccess, NfsPolicyRulePermission,
                                    NfsPolicyRuleSecurity, PolicyType)

FA_API_TOKEN = os.environ.get('FA_API_TOKEN')
FA_ARRAY_HOST = os.environ.get('FA_ARRAY_HOST')

fa = FlashArray(FA_API_TOKEN, FA_ARRAY_HOST)
fa.authenticate()


@pytest.mark.dependency()
def test_create_pod():
    pods = fa.create_pod(name='zsvoboda-pod')
    assert (any(p.name == 'zsvoboda-pod' for p in pods))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_get_pods():
    pods = fa.get_pods()
    assert (any(p.name == 'zsvoboda-pod' for p in pods))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_get_pod():
    pods = fa.get_pod(name='zsvoboda-pod')
    assert (any(p.name == 'zsvoboda-pod' for p in pods))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_file_system():
    fss = fa.create_file_system(name='zsvoboda-pod::zsvoboda-fs')
    assert (any(fs.name == 'zsvoboda-pod::zsvoboda-fs' for fs in fss))


@pytest.mark.dependency(depends=['test_create_pod', 'test_create_file_system'])
def test_get_file_systems():
    fss = fa.get_file_systems()
    assert (any(fs.name == 'zsvoboda-pod::zsvoboda-fs' for fs in fss))


@pytest.mark.dependency(depends=['test_create_pod', 'test_create_file_system'])
def test_get_file_system():
    fss = fa.get_file_system(name='zsvoboda-pod::zsvoboda-fs')
    assert (any(fs.name == 'zsvoboda-pod::zsvoboda-fs' for fs in fss))


@pytest.mark.dependency(depends=['test_create_pod', 'test_create_file_system'])
def test_create_managed_directory():
    mds = fa.create_managed_directory(name='zsvoboda-md', file_system_name='zsvoboda-pod::zsvoboda-fs')
    assert (any(md.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for md in mds))


@pytest.mark.dependency(depends=['test_create_pod', 'test_create_file_system', 'test_create_managed_directory'])
def test_get_managed_directories():
    mds = fa.get_managed_directories()
    assert (any(md.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for md in mds))
    # from a specific file system
    mds = fa.get_managed_directories(file_system_name='zsvoboda-pod::zsvoboda-fs')
    assert (any(md.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for md in mds))


@pytest.mark.dependency(depends=['test_create_pod', 'test_create_file_system', 'test_create_managed_directory'])
def test_get_managed_directory():
    mds = fa.get_managed_directory(name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(md.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for md in mds))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_nfs_policy():
    ps = fa.create_policy_nfs(name='zsvoboda-pod::zsvoboda-policy-nfs')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-nfs' for p in ps))
    psr = fa.create_policy_nfs_rule(nfs_version='nfsv4',
                                    security='krb5p',
                                    policy_name='zsvoboda-pod::zsvoboda-policy-nfs')
    assert (any(p.nfs_version[0] == 'nfsv4' and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-nfs'
                and p.security[0] == 'krb5p' for p in psr))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_smb_policy():
    ps = fa.create_policy_smb(name='zsvoboda-pod::zsvoboda-policy-smb')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-smb' for p in ps))
    psr = fa.create_policy_smb_rule(smb_encryption_required=True, anonymous_access_allowed=True,
                                    policy_name='zsvoboda-pod::zsvoboda-policy-smb')
    assert (any(p.smb_encryption_required and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-smb'
                and p.anonymous_access_allowed for p in psr))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_quota_policy():
    ps = fa.create_policy_quota(name='zsvoboda-pod::zsvoboda-policy-quota')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-quota' for p in ps))
    # TODO: enforced = False is not working
    psr = fa.create_policy_quota_rule(quota_limit=100000, enforced=True,
                                      policy_name='zsvoboda-pod::zsvoboda-policy-quota')
    assert (any(p.policy.name == 'zsvoboda-pod::zsvoboda-policy-quota' and p.enforced
                and p.quota_limit == 100000 for p in psr))


@pytest.mark.dependency(depends=['test_create_nfs_policy', 'test_create_smb_policy', 'test_create_quota_policy'])
def test_get_policy_rules():
    # NFS
    ps = fa.get_policy_nfs_rules()
    assert (any(p.nfs_version[0] == 'nfsv4' and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-nfs'
                and p.security[0] == 'krb5p' for p in ps))
    # NFS for a specific policy
    ps = fa.get_policy_nfs_rules(policy_name='zsvoboda-pod::zsvoboda-policy-nfs')
    assert (any(p.nfs_version[0] == 'nfsv4' and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-nfs'
                and p.security[0] == 'krb5p' for p in ps))
    # SMB
    ps = fa.get_policy_smb_rules()
    assert (any(p.smb_encryption_required and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-smb'
                and p.anonymous_access_allowed for p in ps))
    # SMB for a specific policy
    ps = fa.get_policy_smb_rules(policy_name='zsvoboda-pod::zsvoboda-policy-smb')
    assert (any(p.smb_encryption_required and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-smb'
                and p.anonymous_access_allowed for p in ps))
    # Quota
    ps = fa.get_policy_quota_rules()
    assert (any(p.policy.name == 'zsvoboda-pod::zsvoboda-policy-quota' and p.enforced
                and p.quota_limit == 100000 for p in ps))
    # Quota for a specific policy
    ps = fa.get_policy_quota_rules(policy_name='zsvoboda-pod::zsvoboda-policy-quota')
    assert (any(p.policy.name == 'zsvoboda-pod::zsvoboda-policy-quota' and p.enforced
                and p.quota_limit == 100000 for p in ps))

    # Snapshot
    ps = fa.get_policy_snapshot_rules()
    assert (any(p.name == 'r_1' and p.policy.name == 'snap-daily' for p in ps))
    # Snapshot for a specific policy
    ps = fa.get_policy_snapshot_rules(policy_name='snap-daily')
    assert (any(p.name == 'r_1' and p.policy.name == 'snap-daily' for p in ps))


@pytest.mark.dependency(depends=['test_create_nfs_policy'])
def test_get_policies():
    ps = fa.get_policies()
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-nfs' for p in ps))


def test_get_policy():
    ps = fa.get_policy(name='nfs41_krb5')
    assert (any(p.name == 'nfs41_krb5' for p in ps))


def test_get_directory_exports():
    des = fa.get_directory_exports()
    assert (any(de.export_name == 'zsvoboda_krb5p' for de in des))


def test_get_directory_export():
    des = fa.get_directory_export(name='zsvoboda_krb5p')
    assert (any(de.export_name == 'zsvoboda_krb5p' for de in des))


@pytest.mark.dependency(depends=['test_create_managed_directory'])
def test_delete_managed_directory():
    fa.delete_managed_directory(name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')


@pytest.mark.dependency(depends=['test_create_file_system', 'test_delete_managed_directory'])
def test_delete_file_system():
    fss = fa.destroy_file_system(name='zsvoboda-pod::zsvoboda-fs')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-fs' for p in fss))
    fa.eradicate_file_system(name='zsvoboda-pod::zsvoboda-fs')


@pytest.mark.dependency(depends=['test_create_pod', 'test_delete_file_system'])
def test_delete_pod():
    pods = fa.destroy_pod(name='zsvoboda-pod')
    assert (any(p.name == 'zsvoboda-pod' for p in pods))
    fa.eradicate_pod(name='zsvoboda-pod')
