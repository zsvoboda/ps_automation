from purestorage.automation.flash_array import FlashArray

from enum import Enum


class PolicyType(Enum):
    NFS = 'nfs'
    SMB = 'smb'
    SNAPSHOT = 'snapshot'
    QUOTA = 'quota'
    AUTODIR = 'autodir'


class NfsPolicyRuleAccess(Enum):
    NO_SQUASH = 'no-squash'
    ROOT_SQUASH = 'root-squash'
    ALL_SQUASH = 'all-squash'


class NfsPolicyRulePermission(Enum):
    READ = 'r'
    WRITE = 'w'
    READ_WRITE = 'rw'


class NfsPolicyRuleVersion(Enum):
    NFSV3 = 'nfsv3'
    NFSV41 = 'nfsv41'


class NfsPolicyRuleSecurity(Enum):
    AUTH_SYS = 'auth_sys'
    KRB5 = 'krb5'
    KRB5I = 'krb5i'
    KRB5P = 'krb5p'

