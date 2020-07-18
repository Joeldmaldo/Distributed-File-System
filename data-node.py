###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#
# Student: Joel Maldonado Rivera
# Student ID: 801-14-3804
# File: data-node.py

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	
	# Fill code	
	
	# We create the socket we will be using
	
	created_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# We use the try to attempt to connect to the server
	# We prepare an error handle just in case the connection does not occur.
	try:
		created_socket.connect((meta_ip, meta_port))
		
	except socket.error, e:

		print ("Connection to the metadata server failed. Error: " + str(e))

		sys.exit(1)

	#if no error occured connection was succesful!

	print( "Connection to metadata server was succesful." )


	# We use the following try to determine what happens with the registration. If there was an error, if it was duplicate or if it worked!
	try:
		response = "NAK"
		
		create_packet = Packet()
		
		while response == "NAK":
		
			create_packet.BuildRegPacket(data_ip, data_port)
			
			created_socket.sendall(create_packet.getEncodedPacket())
			
			response = created_socket.recv(1024)

			if response == "DUP":
				print "Duplicate Registration"
				
			# we add the response condition "ACK"
			
			if response == "ACK":
			    print ("Registration process was completed!")

		 	if response == "NAK":
		 		print("error here")
				print "Registratation ERROR"

	finally:
		created_socket.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""

		fname, fsize = p.getFileInfo()
		
		#the below line was commented from the original file
		#self.request.send("OK")

		# Generates an unique block id.
		blockid = str(uuid.uuid1())
		# we add the request send ..
		self.request.send(blockid)
		


		# Open the file for the new data block.  
		# Receive the data block.
		# Send the block id back

		# Fill code
		
		file_info = self.request.recv(1024)    #we receive the file information
		
		
		while len(file_info) < int(fsize):     #while loop used in order to add all of the data 
		    file_info += self.request.recv(1024)
		    
		#After this we open the file 
		
		open_file= open("%s/%s" % (DATA_PATH, blockid), 'w' )
		
		open_file.write(file_info)
		
		open_file.close()
		
		

	def handle_get(self, p):
		
		# Get the block id from the packet
		blockid = p.getBlockID()


		# Read the file with the block id data
		# Send it back to the copy client.
		
		# Fill code
		open_file = open("%s/%s" % (DATA_PATH, blockid), 'r')  # we open the file and get the data of it. We also get the size of the file
		
		file_data = open_file.read()
		file_size = len(file_data)
		
		self.request.sendall('%s|%s' % (file_size, file_data)) # we send the data and size

	def handle(self):
		msg = self.request.recv(1024)
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv )> 4:
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH

			usage()
	except:
		usage()


	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
 	server.serve_forever()
