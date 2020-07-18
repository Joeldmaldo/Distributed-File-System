###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#
# Student: Joel Maldonado Rivera
# Student ID: 801-14-3804
# File: copy.py

import socket
import sys
import os.path

from Packet import *



def connection (getPacks, ipaddress, port):
	
	created_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# we try to connect to the metadata server
	
	try:
		created_socket.connect((ipaddress, int(port)))
	except socket.error, e:
		print (" Connection to the metadataserver failed. Error: " + str(e))
		sys.exit(1)
	
	#if error doesn't occur then connection was succesful
	
	
	
	print " Connection was succesful"
	print "Connection to the ip address " + str(ipaddress) + " on the port : " + str(port) # We print the address and port being used
	
	#since the connection was succesful we do the sendall
	
	created_socket.sendall(getPacks)
	
	reply = created_socket.recv(1024)
	
	# we return the following so we can use the reply and socket used
	
	return reply, created_socket

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""
#We create a function that connects to the metadata server and gives back it;s response



	# Create a connection to the data server

	# Fill code
	
	# We create the connection to the data server
	
	created_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# We use the try to attempt to connect to the server

	try:
	    created_socket.connect((address[0],address[1]))
	except socket.error, e:
		print "Connection to the data server failed. \n %s" % e
		#If connection fails exit
		sys.exit(1)
	#if no error occured then it connected!
	
	print "Connection to the data server was done succesfully!"

	# Read file
	# We open the file, take the file data and file size in the variables so we can use these.
	
	open_file = open( path, 'r')
	
	file_data = open_file.read()
	
	file_size = len(file_data)

	# Fill code

	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
	# Fill code
	
	# We use this try to determine if the file already exits/error occurs or if we proceed and decode the reply and get the data nodes.

	try:
		reply = "DUP"
		
		create_packet = Packet()
		
		while reply == "DUP":
		
			create_packet.BuildPutPacket( fname, file_size)
			
			created_socket.sendall(create_packet.getEncodedPacket())
			
			reply = created_socket.recv(1024)
			
			if reply != "DUP":
			
			    create_packet.DecodePacket(reply)
			    
			    server = create_packet.getDataNodes()
			if reply == "DUP":
			
				print "File already exists or an error occured."
				sys.exit(1)
				
	
			
	finally:
		created_socket.close()		     # afterwards we close the socket we used when the try ends

	# If no error or file exists
	# Get the list of data nodes.
	# Divide the file in blocks
	# Send the blocks to the data servers

	# Fill code	
	
	#We calculate the blocksizes we will be dealing with
	
	file_blocks = file_size / len (server)
	
	leftover_blocks = file_size % len(server)  # we use the module in order to get any extra data that isn't perfectly divisible
	
	block_list = []                            # We create the block list
	
	open_file.seek(0)

	# for loop in which we will be used to send the block information through the socket.

	 
	for x, node in enumerate(server):
		
		block_info = open_file.read(file_blocks)
		if x == len(server) -1:
		
			block_info += open_file.read(leftover_blocks)
			file_blocks += leftover_blocks
		
		create_packet.BuildPutPacket(fname, file_blocks)
		
		receive_data, the_socket = connection(create_packet.getEncodedPacket(),node[0], node[1])
		
		if receive_data:
		
			the_socket.sendall(block_info)
			
		else:
			print "node error"
		
		block_list.append((node[0], str(node[1]), receive_data))
		the_socket.close()
		
	# Notify the metadata server where the blocks are saved.

	# Fill code
	
	
	create_packet.BuildDataBlockPacket(fname, block_list)
	
	worked, the_socket = connection( create_packet.getEncodedPacket(), address[0], address[1])
	
	#if it occured then close
	
	the_socket.close()
	
	worked_2 = int(worked)
	
	if worked_2:
	
		pass
	
	else:
		print("Error occured")
		sys.exit(1)
		
	open_file.close()




def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

	create_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)   # We create the socket
	
    #try for connection

	try:
		create_socket.connect((address[0] , address[1]))
	except socket.error , e:
	
		print ( "Connection to the server failed. Error: " + str(e) )
		sys.exit(1)
	print ( "Connection to the data server was completed with no errors!" )
   	# Contact the metadata server to ask for information of fname
	
	# Fill code

	# try  will create a packet and a socket. EncodedPacket is sent throught the socket. We get a reply.
	# Decode the packet 
	# Get the data nodes from the packet
	
	try: 
		created_packet = Packet()
		
		created_packet.BuildGetPacket(fname)
		
		create_socket.sendall(created_packet.getEncodedPacket())
		
		reply = create_socket.recv(1024)
		
		print("Reply is:")
		print(reply)

		created_packet.DecodePacket(reply)
		print("after decoding:")
		print(reply)
		
		data_nodes = created_packet.getDataNodes()
		
		if reply == "NFOUND":
			print(" File was not found in the server. System will now exit.")
			sys.exit(1)
	finally:
	
		create_socket.close() # used socket in the try was used



	# If there is no error response Retreive the data blocks

	# Fill code
	
	
	open_file = open(path , 'w')          #open the file

	# for loop we create variables that will hold the node's location, the port being used by the node and the block id of it.
	# using the packet that was created we get the datablock packet by using the id.

	# we use the function connection which we created earlier in order to get a reply and a socket to use for communication

	# we get the size and the block data from the reply
	
	for x in data_nodes:
	
		node_location = x[0]
		
		node_port_used = x[1]
		
		retrieved_block_id =x[2]
		
		created_packet.BuildGetDataBlockPacket(retrieved_block_id)
		
		reply , connection_socket = connection(created_packet.getEncodedPacket(), node_location, node_port_used)
		
		size, block_data = reply.split('|', 1)
		
		
		while len(block_data) < int(size):
			
			block_data += connection_socket.recv(1024)
			
    	# Save the file
	
		open_file.write(block_data)
		
		
	open_file.close()
	
	connection_socket.close()
	
	
	# Fill code

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


