import os
import pytest

from purestorage import FlashArray

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


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_snapshot_policy():
    ps = fa.create_policy_snapshot(name='zsvoboda-pod::zsvoboda-policy-snapshot')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-snapshot' for p in ps))
    psr = fa.create_policy_snapshot_rule(client_name='zsvoboda', at=6 * 3600 * 1000, every=24 * 3600 * 1000,
                                         keep_for=48 * 3600 * 1000,
                                         rule_name=None, policy_name='zsvoboda-pod::zsvoboda-policy-snapshot')
    assert (any(p.policy.name == 'zsvoboda-pod::zsvoboda-policy-snapshot' and p.at == 6 * 3600 * 1000
                and p.every == 24 * 3600 * 1000 and p.keep_for == 48 * 3600 * 1000 for p in psr))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_autodir_policy():
    ps = fa.create_policy_autodir(name='zsvoboda-pod::zsvoboda-policy-autodir')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-autodir' for p in ps))


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_and_delete_policy():
    ps = fa.create_policy(name='zsvoboda-pod::zsvoboda-policy-autodir-g', policy_type='autodir')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-autodir-g' for p in ps))
    fa.delete_policy(policy_name='zsvoboda-pod::zsvoboda-policy-autodir-g')


@pytest.mark.dependency(depends=['test_create_pod'])
def test_create_and_delete_policy_rules():
    ps = fa.create_policy_nfs(name='zsvoboda-pod::zsvoboda-policy-nfs-g')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-nfs-g' for p in ps))
    psr = fa.create_policy_nfs_rule(nfs_version='nfsv4',
                                    security='krb5p',
                                    policy_name='zsvoboda-pod::zsvoboda-policy-nfs-g')
    assert (any(p.nfs_version[0] == 'nfsv4' and p.policy.name == 'zsvoboda-pod::zsvoboda-policy-nfs-g'
                and p.security[0] == 'krb5p' for p in psr))
    rules = fa.get_policy_nfs_rules(policy_name='zsvoboda-pod::zsvoboda-policy-nfs-g')
    first_rule = rules.next()
    fa.delete_policy_rules(rule_name=first_rule.name, policy_name='zsvoboda-pod::zsvoboda-policy-nfs-g')
    fa.delete_policy(policy_name='zsvoboda-pod::zsvoboda-policy-nfs-g')


@pytest.mark.dependency(depends=['test_create_nfs_policy', 'test_create_smb_policy', 'test_create_quota_policy',
                                 'test_create_snapshot_policy', 'test_create_autodir_policy'])
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
    assert (any(p.policy.name == 'zsvoboda-pod::zsvoboda-policy-snapshot' and p.at == 6 * 3600 * 1000
                and p.every == 24 * 3600 * 1000 and p.keep_for == 48 * 3600 * 1000 for p in ps))
    # Snapshot for a specific policy
    ps = fa.get_policy_snapshot_rules(policy_name='zsvoboda-pod::zsvoboda-policy-snapshot')
    assert (any(p.policy.name == 'zsvoboda-pod::zsvoboda-policy-snapshot' and p.at == 6 * 3600 * 1000
                and p.every == 24 * 3600 * 1000 and p.keep_for == 48 * 3600 * 1000 for p in ps))


@pytest.mark.dependency(depends=['test_create_nfs_policy'])
def test_get_policies():
    ps = fa.get_policies()
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-nfs' for p in ps))


@pytest.mark.dependency(depends=['test_create_nfs_policy'])
def test_get_policy():
    ps = fa.get_policy(policy_name='zsvoboda-pod::zsvoboda-policy-nfs')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-policy-nfs' for p in ps))


@pytest.mark.dependency(depends=['test_create_managed_directory', 'test_create_nfs_policy'])
def test_export_managed_directory_nfs():
    ps = fa.export_managed_directory_nfs('zsvoboda-export-nfs',
                                         policy_name='zsvoboda-pod::zsvoboda-policy-nfs',
                                         managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_create_managed_directory', 'test_create_smb_policy'])
def test_export_managed_directory_smb():
    ps = fa.export_managed_directory_smb('zsvoboda-export-smb',
                                         policy_name='zsvoboda-pod::zsvoboda-policy-smb',
                                         managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_create_managed_directory', 'test_create_quota_policy'])
def test_add_managed_directory_quota():
    ps = fa.add_managed_directory_quota(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md',
                                        policy_name='zsvoboda-pod::zsvoboda-policy-quota')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_create_managed_directory', 'test_create_snapshot_policy'])
def test_add_managed_directory_snapshot():
    ps = fa.add_managed_directory_snapshot(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md',
                                           policy_name='zsvoboda-pod::zsvoboda-policy-snapshot')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_create_managed_directory', 'test_create_autodir_policy'])
def test_add_managed_directory_autodir():
    ps = fa.add_managed_directory_autodir(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md',
                                          policy_name='zsvoboda-pod::zsvoboda-policy-autodir')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_export_managed_directory_nfs'])
def test_get_managed_directory_policies_nfs():
    ps = fa.get_managed_directory_policies_nfs(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' and
                p.export_name == 'zsvoboda-export-nfs' for p in ps))


@pytest.mark.dependency(depends=['test_export_managed_directory_smb'])
def test_get_managed_directory_policies_smb():
    ps = fa.get_managed_directory_policies_smb(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' and
                p.export_name == 'zsvoboda-export-smb' for p in ps))


@pytest.mark.dependency(depends=['test_add_managed_directory_quota'])
def test_get_managed_directory_policies_quota():
    ps = fa.get_managed_directory_policies_quota(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_add_managed_directory_snapshot'])
def test_get_managed_directory_policies_snapshot():
    ps = fa.get_managed_directory_policies_snapshot(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_add_managed_directory_autodir'])
def test_get_managed_directory_policies_autodir():
    ps = fa.get_managed_directory_policies_autodir(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


def test_get_managed_directory_policies():
    ps = fa.get_managed_directory_policies(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')
    assert (any(p.member.name == 'zsvoboda-pod::zsvoboda-fs:zsvoboda-md' for p in ps))


@pytest.mark.dependency(depends=['test_export_managed_directory_nfs'])
def test_get_directory_exports():
    des = fa.get_directory_exports()
    assert (any(de.export_name == 'zsvoboda-export-nfs' for de in des))


@pytest.mark.dependency(depends=['test_export_managed_directory_nfs'])
def test_get_directory_export():
    des = fa.get_directory_export(name='zsvoboda-export-nfs')
    assert (any(de.export_name == 'zsvoboda-export-nfs' for de in des))


@pytest.mark.dependency(depends=['test_export_managed_directory_nfs', 'test_get_directory_exports',
                                 'test_get_directory_export', 'test_get_managed_directory_policies_nfs'])
def test_delete_nfs_export():
    fa.delete_export('zsvoboda-export-nfs')


@pytest.mark.dependency(depends=['test_export_managed_directory_smb', 'test_get_directory_exports',
                                 'test_get_directory_export', 'test_get_managed_directory_policies_smb'])
def test_delete_smb_export():
    fa.delete_export('zsvoboda-export-smb')


@pytest.mark.dependency(depends=['test_add_managed_directory_quota', 'test_get_managed_directory_policies_quota'])
def test_remove_managed_directory_quota():
    fa.remove_managed_directory_quota_policy(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md',
                                             policy_name='zsvoboda-pod::zsvoboda-policy-quota')


@pytest.mark.dependency(depends=['test_add_managed_directory_snapshot', 'test_get_managed_directory_policies_snapshot'])
def test_remove_managed_directory_snapshot():
    fa.remove_managed_directory_snapshot_policy(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md',
                                                policy_name='zsvoboda-pod::zsvoboda-policy-snapshot')


@pytest.mark.dependency(depends=['test_add_managed_directory_autodir', 'test_get_managed_directory_policies_autodir'])
def test_remove_managed_directory_autodir():
    fa.remove_managed_directory_autodir_policy(managed_directory_name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md',
                                               policy_name='zsvoboda-pod::zsvoboda-policy-autodir')


@pytest.mark.dependency(depends=['test_get_policies', 'test_get_policy_rules', 'test_get_policy',
                                 'test_get_directory_exports', 'test_get_directory_export', 'test_delete_nfs_export'])
def test_delete_nfs_policy_rule():
    rules = fa.get_policy_nfs_rules(policy_name='zsvoboda-pod::zsvoboda-policy-nfs')
    first_rule = rules.next()
    fa.delete_policy_nfs_rules(rule_name=first_rule.name, policy_name='zsvoboda-pod::zsvoboda-policy-nfs')


@pytest.mark.dependency(depends=['test_get_policies', 'test_get_policy_rules', 'test_get_policy',
                                 'test_get_directory_exports', 'test_get_directory_export', 'test_delete_smb_export'])
def test_delete_smb_policy_rule():
    rules = fa.get_policy_smb_rules(policy_name='zsvoboda-pod::zsvoboda-policy-smb')
    first_rule = rules.next()
    fa.delete_policy_smb_rules(rule_name=first_rule.name, policy_name='zsvoboda-pod::zsvoboda-policy-smb')


@pytest.mark.dependency(depends=['test_get_policies', 'test_get_policy_rules', 'test_get_policy',
                                 'test_get_directory_exports', 'test_get_directory_export',
                                 'test_remove_managed_directory_quota'])
def test_delete_quota_policy_rule():
    rules = fa.get_policy_quota_rules(policy_name='zsvoboda-pod::zsvoboda-policy-quota')
    first_rule = rules.next()
    fa.delete_policy_quota_rules(rule_name=first_rule.name, policy_name='zsvoboda-pod::zsvoboda-policy-quota')


@pytest.mark.dependency(depends=['test_get_policies', 'test_get_policy_rules', 'test_get_policy',
                                 'test_get_directory_exports', 'test_get_directory_export',
                                 'test_remove_managed_directory_snapshot'])
def test_delete_snapshot_policy_rule():
    rules = fa.get_policy_snapshot_rules(policy_name='zsvoboda-pod::zsvoboda-policy-snapshot')
    first_rule = rules.next()
    fa.delete_policy_snapshot_rules(rule_name=first_rule.name, policy_name='zsvoboda-pod::zsvoboda-policy-snapshot')


@pytest.mark.dependency(depends=['test_delete_nfs_policy_rule'])
def test_delete_nfs_policy():
    fa.delete_policy_nfs(name='zsvoboda-pod::zsvoboda-policy-nfs')


@pytest.mark.dependency(depends=['test_delete_smb_policy_rule'])
def test_delete_smb_policy():
    fa.delete_policy_smb(name='zsvoboda-pod::zsvoboda-policy-smb')


@pytest.mark.dependency(depends=['test_delete_quota_policy_rule'])
def test_delete_quota_policy():
    fa.delete_policy_quota(name='zsvoboda-pod::zsvoboda-policy-quota')


@pytest.mark.dependency(depends=['test_delete_snapshot_policy_rule'])
def test_delete_snapshot_policy():
    fa.delete_policy_snapshot(name='zsvoboda-pod::zsvoboda-policy-snapshot')


@pytest.mark.dependency(depends=['test_get_policies', 'test_get_policy_rules', 'test_get_policy',
                                 'test_get_directory_exports', 'test_get_directory_export',
                                 'test_remove_managed_directory_autodir'])
def test_delete_autodir_policy():
    fa.delete_policy_autodir(name='zsvoboda-pod::zsvoboda-policy-autodir')


@pytest.mark.dependency(depends=['test_delete_nfs_policy_rule'])
def test_delete_managed_directory():
    fa.delete_managed_directory(name='zsvoboda-pod::zsvoboda-fs:zsvoboda-md')


@pytest.mark.dependency(depends=['test_create_file_system', 'test_delete_managed_directory'])
def test_delete_file_system():
    fss = fa.destroy_file_system(name='zsvoboda-pod::zsvoboda-fs')
    assert (any(p.name == 'zsvoboda-pod::zsvoboda-fs' for p in fss))
    fa.eradicate_file_system(name='zsvoboda-pod::zsvoboda-fs')


@pytest.mark.dependency(depends=['test_create_pod', 'test_delete_file_system'])
def test_delete_pod():
    pods = fa.destroy_pod(name='zsvoboda-pod', destroy_contents=True)
    assert (any(p.name == 'zsvoboda-pod' for p in pods))
    fa.eradicate_pod(name='zsvoboda-pod', eradicate_contents=True)
