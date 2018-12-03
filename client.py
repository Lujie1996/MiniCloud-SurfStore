import rpyc
import hashlib
import os
import sys
import time
import copy
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
        self.hash_block = dict() # SHAhashkey -> [block1, block2, ..]

        configuration = parse_config(config)
        self.no_of_block_stores = configuration[0]
        self.metadata = configuration[1]
        self.blockstores = configuration[2]
        self.block_replacement_algorithm = configuration[3]

        self.server_no = -1

        self.metadata_conn = rpyc.connect(self.metadata["host"], self.metadata["port"],
                                          config={"allow_all_attrs": True, "instantiate_custom_exceptions": True,  'allow_pickle': True})
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

        server_version = self.metadata_conn.root.read_file(filename)[0]

        # first modify()
        hashlist_to_send = list()
        for key in self.hash_block:
            server_no = self.get_block_location(key)
            row = [key, server_no]
            hashlist_to_send.append(row)

        missing_blocks = list()
        try:
            self.metadata_conn.root.modify_file(filename, int(server_version)+1, hashlist_to_send)
            print("OK")
            return
        except Exception as e:
            # extract version and missing blocks from msg
            new_server_version = int(server_version)
            if e.error_type == 1:
                # missing blocks
                missing_blocks = list(eval(e.missing_blocks))
                # missing_blocks = list(e.missing_blocks)
            elif e.error_type == 2:
                # version error
                new_server_version = int(e.current_version)
        
        # send missing blocks to blockstore
        for key in missing_blocks:
            server_no = self.get_block_location(key)
            self.blockstore_conns[server_no].root.store_block(key, self.hash_block[key])

        # second modify()
        try:
            self.metadata_conn.root.modify_file(filename, new_server_version+1, hashlist_to_send)
            print("OK")
            return
        except:
            print("Second modify() call failed.")

    """
        delete(filename) : Signals the MetadataStore to delete a file.
    """

    def delete(self, filename):
        server_version = self.metadata_conn.root.read_file(filename)[0]
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
        # server_hashlist = copy.deepcopy(server_hashlist)

        # when file does not exist
        if len(server_hashlist) == 0:
            print("Not Found")
            return

        missing_blocks = list()
        for row in server_hashlist:
            key = row[0]
            server_no = row[1]
            if key not in self.hash_block:
                missing_blocks.append([key, server_no])

        # fetch every individual blocks from blockstore
        for row in missing_blocks:
            key = row[0]
            server_no = row[1]
            msg = self.blockstore_conns[server_no].root.get_block(key)
            self.hash_block[key] = msg

        # merge blocks
        content = b""
        for key in self.hash_block:
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

    def get_block_location(self, block):
        if self.block_replacement_algorithm == 0:
            # hash-based
            return int(block,16) % self.no_of_block_stores
        elif self.block_replacement_algorithm == 1:
            # nearest to client
            if self.server_no != -1:
                return self.server_no
            else:
                minRTT = 99999
                for server in range(self.no_of_block_stores):
                    thisRTT = self.get_RTT(server)
                    print("RTT of BlockStore Server #" + str(server) + ": " + str(thisRTT))
                    if  thisRTT < minRTT:
                        minRTT = thisRTT
                        self.server_no = server
            print("Nearest BlockStore Server: #" + str(self.server_no))
            return self.server_no

    def get_RTT(self, server_no):
        sum = 0
        for i in range(5):
            t0 = time.time()
            self.blockstore_conns[server_no].root.ping()
            t1 = time.time()
            sum += (t1 - t0)
        return sum / 5

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
