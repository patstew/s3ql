try: #Ignore ImportError if acd_cli is not installed
    import os
    from configparser import ConfigParser
    import threading
    import tempfile
    import hashlib
    import time
    import json

    from dugong import is_temp_network_error
    from .. import BUFSIZE
    from .common import (AbstractBackend, get_ssl_context, get_proxy, NoSuchObject,
                        retry, retry_generator)
    from ..logging import logging, QuietError
    from ..common import (freeze_basic_mapping, thaw_basic_mapping)
    from ..inherit_docstrings import copy_ancestor_docstring, ABCDocstMeta

    import appdirs
    import acdcli
    from acdcli.api import client
    from acdcli.api.common import RequestError
    from acdcli.cache import db
    from acdcli.utils.conf import get_conf
    from acdcli.cache.db import CacheConsts
    
    log = logging.getLogger(__name__)

    S3QL_PROPERTY_METADATA = 's3ql'

    class Backend(AbstractBackend, metaclass=ABCDocstMeta):
        """A backend to store data in Amazon Cloud Drive based on acd_cli."""

        known_options = {}

        _static_lock = threading.Lock()
        _acd_client = None
        _acd_cache = None
        _acd_conf = None

        def __init__(self, storage_url, login, password, options):
            # Unused argument
            #pylint: disable=W0613

            super().__init__()
            
            if not storage_url.startswith("acd://"):
                raise QuietError('Invalid storage URL', exitcode=2)
            
            with Backend._static_lock:
                if Backend._acd_client is None:
                    # acd_cli path settings copied from acd_cli
                    _app_name = 'acd_cli'
                    cp = os.environ.get('ACD_CLI_CACHE_PATH')
                    sp = os.environ.get('ACD_CLI_SETTINGS_PATH')

                    CACHE_PATH = cp if cp else appdirs.user_cache_dir(_app_name)
                    SETTINGS_PATH = sp if sp else appdirs.user_config_dir(_app_name)

                    _SETTINGS_FILENAME = _app_name + '.ini'
                    
                    def_conf = ConfigParser()
                    def_conf['download'] = dict(keep_corrupt=False, keep_incomplete=True)
                    def_conf['upload'] = dict(timeout_wait=10)
                    
                    Backend._acd_conf = get_conf(SETTINGS_PATH, _SETTINGS_FILENAME, def_conf)

                    Backend._acd_client = client.ACDClient(CACHE_PATH, SETTINGS_PATH)

                    Backend._acd_cache = db.NodeCache(CACHE_PATH, SETTINGS_PATH)
            
                    Backend.acd_client_owner = Backend._acd_cache.KeyValueStorage.get(CacheConsts.OWNER_ID)
                    
            self.path = storage_url[6:].strip("/")
            
            self.parent_node_id = Backend._acd_cache.get_root_node().id
            
            self._create_rootdir()
           
        @retry
        def _create_rootdir(self):
            if self.path:
                for dir in self.path.split('/'):
                    n = Backend._acd_cache.get_child(self.parent_node_id, dir)
                    if n:
                        if not n.is_folder:
                            raise QuietError('Invalid storage URL', exitcode=2)
                        self.parent_node_id = n.id
                    else:
                        n = Backend._acd_client.create_folder(dir, self.parent_node_id)
                        Backend._acd_cache.insert_node(n)
                        self.parent_node_id = n['id']
        
        # public methods
        @property
        @copy_ancestor_docstring
        def has_native_rename(self):
            return True

        @copy_ancestor_docstring
        def is_temp_failure(self, exc):
            log.warning("Got exception %s: %s" % (type(exc).__name__, str(exc)))
            
            if isinstance(exc, (MD5Error, SizeError)):
                return True
            elif is_temp_network_error(exc):
                return True
            elif (isinstance(exc, RequestError) and
                ((500 <= exc.status_code <= 599
                    and exc.status_code not in (501,505,508,510,511,523))
                or exc.status_code in (400, 401, 408, 429, RequestError.CODE.CONN_EXCEPTION, RequestError.CODE.FAILED_SUBREQUEST, RequestError.CODE.INCOMPLETE_RESULT, RequestError.CODE.REFRESH_FAILED))):
                return True
            
            return False

        @copy_ancestor_docstring
        def clear(self):
            if self.path:
                node = Backend._acd_client.move_to_trash(self.parent_node_id)
                Backend._acd_cache.insert_node(node)
                self.parent_node_id = Backend._acd_cache.get_root_node().id
                self._create_rootdir()
            else:
                raise RuntimeError("Cannot clear s3ql files only if files are in root of the drive")
        
        def get_node_with_metadata(self, key):
            node = Backend._acd_cache.get_child(self.parent_node_id, key)
            if not node:
                raise NoSuchObject(key)
            md = Backend._acd_cache.get_property(node.id, self.acd_client_owner, S3QL_PROPERTY_METADATA)
            return node, md and thaw_basic_mapping(md.encode('utf-8'))
        
        @retry
        @copy_ancestor_docstring
        def lookup(self, key):
            return self.get_node_with_metadata(key)[1]

        @retry
        @copy_ancestor_docstring
        def get_size(self, key):
            node = Backend._acd_cache.get_child(self.parent_node_id, key)
            if not node:
                raise NoSuchObject(key)
            return node.size

        @retry
        @copy_ancestor_docstring
        def open_read(self, key):
            return ObjectR(*self.get_node_with_metadata(key))

        @retry
        @copy_ancestor_docstring
        def open_write(self, key, metadata=None, is_compressed=False):
            return ObjectW(self.parent_node_id, key, self, metadata)

        # At the moment it only moves to trash, apparently it's not possible to
        # permanently delete files from the api...
        @retry
        @copy_ancestor_docstring
        def delete(self, key, force=False, is_retry=False):
            node = Backend._acd_cache.get_child(self.parent_node_id, key)
            if node:
                node = Backend._acd_client.move_to_trash(node.id)
                Backend._acd_cache.insert_node(node)
            else:
                if force or is_retry:
                    pass
                else:
                    raise NoSuchObject(key)

        @retry_generator
        @copy_ancestor_docstring
        def list(self, prefix=''):
            for n in Backend._acd_cache.childrens_names(self.parent_node_id):
                if n.startswith(prefix):
                    yield n

        @retry
        @copy_ancestor_docstring
        def copy(self, src, dst, metadata=None):
            # It's not possible to do remote copy in acd api, download and reupload...
            #log.warning('client side copying from %r to %r' % (src, dst))
            if not metadata:
                metadata = self.lookup(src)

            with self.open_read(src) as srcf:
                with self.open_write(dst, metadata) as dstf:
                    buf = srcf.read(BUFSIZE)
                    while len(buf) > 0:
                        dstf.write(buf)
                        buf = srcf.read(BUFSIZE)

        @copy_ancestor_docstring
        def update_meta(self, key, metadata):
            if metadata is not None:
                node = Backend._acd_cache.get_child(self.parent_node_id, key)
                if not node:
                    raise NoSuchObject(key)
                m = freeze_basic_mapping(metadata).decode('utf-8')
                Backend._acd_client.add_property(node.id, self.acd_client_owner, S3QL_PROPERTY_METADATA, m)
                Backend._acd_cache.insert_property(node.id, self.acd_client_owner, S3QL_PROPERTY_METADATA, m)

        @retry
        @copy_ancestor_docstring
        def rename(self, src, dest, metadata=None):
            node = Backend._acd_cache.get_child(self.parent_node_id, src)
            if not node:
                raise NoSuchObject(src)
            
            # can't overwrite with rename...
            if src != dest:
                self.delete(dest, force=True)

            m = {'name': dest}
            if metadata:
                m['properties'] = {self.acd_client_owner: {S3QL_PROPERTY_METADATA: freeze_basic_mapping(metadata).decode('utf-8')}}

            node = Backend._acd_client.update_metadata(node.id, m)
            if not node:
                raise NoSuchObject(key)
            Backend._acd_cache.insert_node(node)
            if node['name'] != dest or (metadata and m['properties'][self.acd_client_owner][S3QL_PROPERTY_METADATA] != node['properties'][self.acd_client_owner][S3QL_PROPERTY_METADATA]):
                raise RuntimeError('Rename failed for %s (received: %s, sent: %s)' %
                                        (self.key, repr(node), repr(m)))
            
        @retry
        @copy_ancestor_docstring
        def close(self):
            pass

    class ObjectR(object):
        def __init__(self, node, metadata):
            self.req = Backend._acd_client.response_chunk(node.id, 0, node.size)
            self.closed = False
            self.md5_checked = False
            self.md5 = hashlib.md5()
            self.md5_expected = node.md5
            self.metadata = metadata
        
        def read(self, size=None):
            if size == 0:
                return b''
            
            buf = next(self.req.iter_content(size), b'')
            self.md5.update(buf)
            
            # Check MD5 on EOF
            if (not buf or size is None) and not self.md5_checked:
                self.md5_checked = True
                if self.md5_expected != self.md5.hexdigest():
                    raise MD5Error('MD5 mismatch (expected: %s, calculated: %s)' %
                                        (self.md5_expected, self.md5.hexdigest()))
            return buf

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.close()
            return False

        def close(self, checksum_warning=True):
            '''Close object

            If *checksum_warning* is true, this will generate a warning message if
            the object has not been fully read (because in that case the MD5
            checksum cannot be checked).
            '''

            if self.closed:
                return
            self.req.close()
            self.closed = True

            if not self.md5_checked:
                if checksum_warning:
                    log.warning("Object closed prematurely, can't check MD5")
            
    class ObjectW(object):
        '''An object open for writing

        All data is first cached in memory, upload only starts when
        the close() method is called.
        '''

        def __init__(self, parent_id, key, backend, metadata):
            self.parent_id = parent_id
            self.key = key
            self.backend = backend
            self.metadata = metadata
            self.closed = False
            self.obj_size = 0

            # According to http://docs.python.org/3/library/functions.html#open
            # the buffer size is typically ~8 kB. We process data in much
            # larger chunks, so buffering would only hurt performance.
            self.fh = tempfile.TemporaryFile(buffering=0)
            self.md5 = hashlib.md5()

        def write(self, buf):
            '''Write object data'''

            self.fh.write(buf)
            self.md5.update(buf)
            self.obj_size += len(buf)

        def is_temp_failure(self, exc):
            return self.backend.is_temp_failure(exc)

        @retry
        def close(self):
            '''Close object and upload data'''

            log.debug('started with %s', self.key)

            if self.closed:
                # still call fh.close, may have generated an error before
                self.fh.close()
                return

            self.fh.seek(0)
            
            node = Backend._acd_cache.get_child(self.parent_id, self.key)
            if node:
                node = Backend._acd_client.overwrite_stream(self.fh, node.id)
            else:
                #Deal with error 409 (file already exists, but is evidently not in our cache) by overwriting
                try:
                    node = Backend._acd_client.upload_stream(self.fh, self.key, self.parent_id)
                except RequestError as e:
                    if (e.status_code == 409):
                        self.fh.seek(0)
                        node = Backend._acd_client.overwrite_stream(self.fh, json.loads(e.msg)['info']['nodeId'])
                    else:
                        raise
            
            Backend._acd_cache.insert_node(node)
            node = Backend._acd_cache.get_node(node['id'])

            if node.size != self.obj_size:
                # delete may fail, but we don't want to loose the exception
                try:
                    self.backend.delete(self.key)
                finally:
                    raise SizeError('Size mismatch for %s (received: %s, sent: %s)' %
                                        (self.key, node.size, self.obj_size))
                
            if node.md5 != self.md5.hexdigest():
                try:
                    self.backend.delete(self.key)
                finally:
                    raise MD5Error('MD5 mismatch for %s (received: %s, sent: %s)' %
                                        (self.key, node.md5, self.md5.hexdigest()))
            
            self.backend.update_meta(self.key, self.metadata)
            
            self.closed = True
            self.fh.close()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.close()
            return False

        def get_obj_size(self):
            if not self.closed:
                raise RuntimeError('Object must be closed first.')
            return self.obj_size

    class ACDError(Exception): pass
    class MD5Error(ACDError): pass
    class SizeError(ACDError): pass

#We couldn't import acd_cli, rethrow the error if anyone tries to use the backend
except ImportError as e: 
    class Backend(object):
        e = e
        def __init__(self):
            raise self.e
