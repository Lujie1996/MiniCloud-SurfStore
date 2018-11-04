import rpyc
import sys


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
	def __init__(self, message):
		super(ErrorResponse, self).__init__(message)
		self.error = message

	def missing_blocks(self, hashlist):
		self.error_type = 1
		self.missing_blocks = hashlist

	def wrong_version_error(self, version):
		self.error_type = 2
		self.current_version = version

	def file_not_found(self):
		self.error_type = 3



'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):
	

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config):
		self.filename_hashlist = dict() # str -> []
		self.filename_version = dict() # str -> int

		self.tombstone_filename_verstion = dict() # str -> int
		'''
			When file is deleted, it is removed from filename_hashlist and filename_version;
			It is added into tombstone_filename_verstion with associated with its latest version
		'''
	

		lines = list()
		with open(config, "r") as text:
			lines = text.readlines()
		'''
			format:
				B: 1
				metadata: localhost:6000
				block0: localhost:5000
		'''

		self.no_of_block_stores = int(lines[0].split(":")[-1])
		self.metadata["host"] = lines[1].split(": ")[1].split(":")[0] # type: str
		self.metadata["port"] = lines[1].split(": ")[1].split(":")[1] # type: str
		self.blockstores = list() # a list of dicts
		'''
			e.g., blockstores = [{'host': 'localhost', 'port': '6000'}, {'host': 'localhost', 'port': '6000'}]
		'''
		for i in range(self.no_of_block_stores):
			line = lines[2+i]
			t = dict()
			t["host"] = line.split[": "][1].split(":")[0]
			t["port"] = line.split[": "][1].split(":")[1]
			self.blockstores.append(t)

		self.blockstore_conns = list() # list of connections with every blockstore
		for blockstore in self.blockstores:
			host = blockstore["host"]
			port = int(blockstore["port"])
			try:
				conn = rpyc.connect(host, port)
				self.blockstore_conns.append(conn)
			except:
				print("connection failed! (tried to connect with (" + host + "," + port + ") but failed)")

	'''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_modify_file(self, filename, version, hashlist):
		if filename not in self.filename_hashlist:
			self.filename_hashlist[filename] = hashlist
			if filename not in self.tombstone_filename_verstion:
				self.filename_version[filename] = 1
			else:
				self.filename_version[filename] = self.tombstone_filename_verstion[filename] + 1
				del self.tombstone_filename_verstion[filename]
				

		missing_block_list = list()
		try:
			for hashnode in hashlist:
				server_no = int(hashnode,16) % self.no_of_block_stores
				conn = self.blockstore_conns[server_no]
				if not conn.root.has_block(hashnode):
					missing_block_list.append(hashnode)
		except: 
			print("check has_hash() failed")

		if len(missing_block_list) == 0:

			return "OK"
		else:
			return ErrorResponse("Missing Blocks").missing_blocks(tuple(missing_block_list))
			

	'''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_delete_file(self, filename, version):
		pass


	'''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_read_file(self, filename):
		if filename not in self.filename_hashlist:
			return (0, tuple())
		else:
			version = self.filename_version[filename]
			hashlist = self.filename_hashlist[filename]
			return (version, tuple(hashlist))


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
	server.start()

