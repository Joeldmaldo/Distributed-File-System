###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.
#
# Student: Joel Maldonado Rivera
# Student ID: 801-14-3804
# File: meta-data.py
#
# Summary: 
# We fill in the missing code in places such as the handle_list function
#
#
#
from mds_db import *
from Packet import *
import sys
import SocketServer

def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)


class MetadataTCPHandler(SocketServer.BaseRequestHandler):

	def handle_reg(self, db, p):
		"""Register a new client to the DFS  ACK if successfully REGISTERED
			NAK if problem, DUP if the IP and port already registered
		"""
		try:
			if db.AddDataNode(p.getAddr(), p.getPort()): 
				self.request.sendall("ACK") 
			else:
				print("DUP Sent")
				self.request.sendall("DUP")
		except:
			print("fell here 2")
			self.request.sendall("NAK")

	def handle_list(self, db):
		"""Get the file list from the database and send list to client"""
		try:
			# Fill code here
			#filling till 
			
			pack = Packet()    # we create the packet
			file_list = []     # we create a list which will contains the files
			
			for file, size in db.GetFiles():      #for loop which will get the files and their size and this will be appended in the list we created earlier
				
				file_list.append((file,size))
				
			else:

				pack.BuildListResponse(file_list)  # we use the packet we created and builds a registration packet.

				self.request.sendall(pack.getEncodedPacket()) #We use the getEncodedPacket to return a seriliazed packet ready to send through the network and send it.  
			#code filled till here
		except:
			print( "fell here")
			self.request.sendall("NAK")	

	def handle_put(self, db, p):
		"""Insert new file into the database and send data nodes to save
		   the file.
		"""
	       
		# Fill code 
		file_data = p.getFileInfo()  # getFileInfo will return the file info in a packet. We put this in the newly created variable
	
		if db.InsertFile(file_data[0], file_data[1]): 
			# Fill code
			p.BuildPutResponse(db.GetDataNodes()) 
			# GetDataNodes gives us a list of data servers. BuildPutResponse uses this since it 
			#builds a list of data node servers where a file data blocks can be stored

			self.request.sendall(p.getEncodedPacket()) #We use the getEncodedPacket to return a seriliazed packet ready to send through the network and send it.  
			
		else:
			self.request.sendall("DUP")
	
	def handle_get(self, db, p):
		"""Check if file is in database and return list of
			server nodes that contain the file.
		"""

		# Fill code to get the file name from packet and then 
		# get the fsize and array of metadata server
		file_size, meta_array = db.GetFileInode(p.getFileName())

		#getFileName returns the file name in a packet, we get in in order to be albe to use the GetFileInode.
		# We use the file name in a packet in order to get the inode.

		if file_size:
			# Fill code
			p.BuildGetResponse(meta_array, file_size)   
			#builds a list of data node servers where a file data blocks can be stored
			#fill code ends
			self.request.sendall(p.getEncodedPacket())
		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Fill code to get file name and blocks from
		# packet
		File_Name= p.getFileName()         # We get the file's name and save it
		Packet_blocks =p.getDataBlocks()   # We get the datablocks of the given packet and store them in the created variable
		#fill code ends
	
		# Fill code to add blocks to file inode
		
		self.request.sendall(str(db.AddBlockToInode(File_Name,Packet_blocks)))
		#fill code ends
		
	def handle(self):

		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)
	

		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files
			# Fill code
			self.handle_list(db)
			#fill code ends
		
		elif cmd == "put":
			# Client asking for servers to put data
			# Fill code
			self.handle_put(db,p)
			#fill code ends
		
		elif cmd == "get":
			# Client asking for servers to get data
			# Fill code
			self.handle_get(db,p) #justify
			#fill code ends

		elif cmd == "dblks":
			# Client sending data blocks for file
			 # Fill code
			 self.handle_blocks(db,p)
			 #fill code ends


		db.Close()

if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(sys.argv) > 1:
    	try:
    		PORT = int(sys.argv[1])
    	except:
    		usage()

    server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
