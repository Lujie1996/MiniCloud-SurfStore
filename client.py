import rpyc
import hashlib
import os
import sys
from metastore import parse_config, connection_to

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""


class SurfStoreClient():

    """
        Initialize the client and set up connections to the block stores and
        metadata store using the config file
    """

    def __init__(self, config):
        self.hash_block = dict()

        configuration = parse_config(config)
        self.no_of_block_stores = configuration[0]
        self.metadata = configuration[1]
        self.blockstores = configuration[2]

        self.metadata_conn = rpyc.connect(self.metadata["host"], self.metadata["port"],
                                          config={"allow_all_attrs": True, "instantiate_custom_exceptions": True})
        self.blockstore_conns = connection_to(self.blockstores)

    """
        upload(filepath) : Reads the local file, creates a set of 
        hashed blocks and uploads them onto the MetadataStore 
        (and potentially the BlockStore if they were not already present there).
    """

    def upload(self, filepath):

        path = os.path.realpath(filepath)
        filename = path.split('/')[-1]

        if not os.path.isfile(path):
            print("Not Found")
            return

        # read file into many 4096 byte blocks
        with open(path, "rb") as f:
            while True:
                chunck = f.read(4096)
                if chunck:
                    hash_key = hashlib.sha256(chunck).hexdigest()
                    self.hash_block[hash_key] = chunck
                else:
                    break

        server_version, server_hashlist = self.metadata_conn.root.read_file(filename)

        # first modify()
        hashlist_to_send = list()
        for key in self.hash_block:
            hashlist_to_send.append(key)

        try:
            self.metadata_conn.root.modify_file(filename, int(server_version)+1, str(hashlist_to_send))
            print("OK")
            return
        except Exception as e:
            # extract version and missing blocks from msg
            missing_blocks = list()
            new_server_version = int(server_version)
            if e.error_type == 1:
                # missing blocks
                # missing_blocks = list(eval(e.missing_blocks))
                missing_blocks = list(e.missing_blocks)
            elif e.error_type == 2:
                # version error
                new_server_version = int(e.current_version)

        # send missing blocks to blockstore
        for key in missing_blocks:
            server_no = int(key, 16) % self.no_of_block_stores
            self.blockstore_conns[server_no].root.store_block(key, self.hash_block[key])

        # second modify()
        try:
            self.metadata_conn.root.modify_file(filename, new_server_version+1, str(hashlist_to_send))
            print("OK")
            return
        except:
            print("Second modify() call failed.")

    """
        delete(filename) : Signals the MetadataStore to delete a file.
    """

    def delete(self, filename):
        server_version, server_hashlist = self.metadata_conn.root.read_file(filename)
        try:
            self.metadata_conn.root.delete_file(filename, server_version+1)
            print("OK")
            return
        except Exception as e:
            print("Not Found")
            return
            

    """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
    """

    def download(self, filename, location):
        server_version, server_hashlist = self.metadata_conn.root.read_file(filename)
        server_hashlist = list(eval(server_hashlist))

        # when file does not exist
        if len(server_hashlist) == 0:
            print("Not Found")
            return

        missing_blocks = list()
        for key in server_hashlist:
            if key not in self.hash_block:
                missing_blocks.append(key)

        # fetch every individual blocks from blockstore
        for key in missing_blocks:
            server_no = int(key, 16) % self.no_of_block_stores
            msg = self.blockstore_conns[server_no].root.get_block(key)
            self.hash_block[key] = msg

        # merge blocks
        content = b""
        for key in server_hashlist:
            content += self.hash_block[key]

        # write out file
        if not os.path.exists(location):
            print("Location incorrect.")
            return
        full_path = location + "/" + filename

        with open(full_path, "wb") as f:
            f.write(content)

        print("OK")

    """
        Use eprint to print debug messages to stderr
        E.g - 
        self.eprint("This is a debug message")
    """

    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)


if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    operation = sys.argv[2]
    if operation == 'upload':
        client.upload(sys.argv[3])
    elif operation == 'download':
        client.download(sys.argv[3], sys.argv[4])
    elif operation == 'delete':
        client.delete(sys.argv[3])
    else:
        print("Invalid operation")
