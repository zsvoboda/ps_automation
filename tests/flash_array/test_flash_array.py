import os

from purestorage.automation import FlashArray

FA_API_TOKEN = os.environ.get('FA_API_TOKEN')
FA_ARRAY_HOST = os.environ.get('FA_ARRAY_HOST')

fa = FlashArray(FA_API_TOKEN, FA_ARRAY_HOST)
fa.authenticate()


def test_get_pods():
    pods = fa.get_pods()
    assert (any(p.name == 'filerepA' for p in pods))

def test_get_pod():
    pods = fa.get_pod(name='filerepA')
    assert (any(p.name == 'filerepA' for p in pods))

def test_get_file_systems():
    fss = fa.get_file_systems()
    assert (any(fs.name == 'zsvoboda' for fs in fss))


def test_get_file_system():
    fss = fa.get_file_system(name='zsvoboda')
    assert (any(fs.name == 'zsvoboda' for fs in fss))


def test_create_file_system():
    fss = fa.create_file_system(name='zsvoboda_from_api_3')
    assert (any(fs.name == 'zsvoboda_from_api_3' for fs in fss))

def test_create_file_system_in_pod():
    fss = fa.create_file_system(name='testpete1::zsvoboda_from_api_6')
    assert(any(fs.name == 'testpete1::zsvoboda_from_api_6' for fs in fss))

def test_get_managed_directories():
    mds = fa.get_managed_directories()
    assert (any(md.name == 'zsvoboda:root' for md in mds))


def test_get_managed_directory():
    mds = fa.get_managed_directory(name='zsvoboda:root')
    assert (any(md.name == 'zsvoboda:root' for md in mds))


def test_get_policies():
    ps = fa.get_policies()
    assert (any(p.name == 'nfs41_krb5' for p in ps))


def test_get_policy():
    ps = fa.get_policy(name='nfs41_krb5')
    assert (any(p.name == 'nfs41_krb5' for p in ps))


def test_get_directory_exports():
    des = fa.get_directory_exports()
    assert (any(de.export_name == 'zsvoboda_krb5p' for de in des))


def test_get_directory_export():
    des = fa.get_directory_export(name='zsvoboda_krb5p')
    assert (any(de.export_name == 'zsvoboda_krb5p' for de in des))


