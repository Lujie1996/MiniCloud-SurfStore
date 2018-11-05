import rpyc
import hashlib
import os
import sys
from metastore import parse_config, connection_to, ErrorResponse

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
        # parse filename
        if '/' in filepath:
            filename = filepath.split('/')[-1]
        else:
            filename = filepath

        if not os.path.exists(filepath):
            print("Not Found")
            return

        # read file into many 4096 byte blocks
        try:
            with open(filepath, "rb") as f:
                while True:
                    chunck = f.read(4086)
                    if chunck:
                        hash_key = hashlib.sha256(chunck).hexdigest()
                        self.hash_block[hash_key] = chunck
                    else:
                        break
        except IOError:
            print("Read local file Error.")
            return
        try:
            server_version, server_hashlist = self.metadata_conn.root.read_file(filename)
            server_hashlist = list(eval(server_hashlist))
        except Exception as e:
            print("read_file error.\n" + str(e))
            return

        # first modify() with metastore
        hashlist_to_send = list()
        for key in self.hash_block:
            hashlist_to_send.append(key)
        try:
            msg = self.metadata_conn.root.modify_file(filename, server_version+1, str(hashlist_to_send))
        except Exception as e:
            print("Error in Connection with MetaStore #1.\n" + str(e))
            return

        # extract version and missing blocks from msg
        missing_blocks = list()
        new_server_version = server_version
        if isinstance(msg, ErrorResponse):
            if msg.error_type == 1:
                # missing blocks
                missing_blocks = msg.missing_blocks
            elif msg.error_type == 2:
                # version error
                new_server_version = msg.current_version
                print("Version Error")
            else:
                print("Unknown error_type.")
        elif msg == "OK":
            print("OK")
            return
        else:
            print("Unknown Error.")

        # send missing blocks to blockstore
        for key in missing_blocks:
            server_no = int(key, 16) % self.no_of_block_stores
            if key in self.hash_block:
                try:
                    msg = self.blockstore_conns[server_no].root.store_block(key, self.hash_block[key])
                    if msg != "OK":
                        print("One block is failed to store.")
                except Exception as e:
                    print(e)
            else:
                print("Local block cannot find.")
                return

        # second modify()
        try:
            msg = self.metadata_conn.root.modify_file(filename, new_server_version+1, str(hashlist_to_send))
        except Exception as e:
            print("Error in Connection with MetaStore #2.\n" + str(e))
            return
        if msg == "OK":
            print("OK")
        else:
            print("Second modify() call failed.")

    """
        delete(filename) : Signals the MetadataStore to delete a file.
    """

    def delete(self, filename):
        try:
            msg = self.metadata_conn.root.delete_file(filename, 1)
        except Exception as e:
            print("Error\n" + str(e))
        if isinstance(msg, ErrorResponse):
            if msg.error_type == 3:
                print("Not Found")
            elif msg.error_type == 2:
                server_version = msg.current_version
                next_msg = self.metadata_conn.root.delete_file(filename, server_version+1)
                if next_msg == "OK":
                    print("OK")
                    return
                else:
                    print("Error")
                    return

    """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
    """

    def download(self, filename, location):
        try:
            server_version, server_hashlist = self.metadata_conn.root.read_file(filename)
            server_hashlist = list(eval(server_hashlist))
        except Exception as e:
            print("read_file error\n" + str(e))
            return

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
            try:
                msg = self.blockstore_conns[server_no].root.get_block(key)
            except:
                print("get_block error")
                return
            if isinstance(msg, ErrorResponse):
                print(msg)
                return
            self.hash_block[key] = msg

        # merge blocks
        content = b""
        for key in server_hashlist:
            if key not in self.hash_block:
                print("Missing block")
                return
            content += self.hash_block[key]

        # write out file
        if not os.path.exists(location):
            print("Location incorrect.")
            return
        full_path = location + "/" + filename
        try:
            with open(full_path, "wb") as f:
                f.write(content)
        except IOError as e:
            print("IO Error.\n" + str(e))
            return
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
