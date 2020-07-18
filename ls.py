###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	List client for the DFS
#
# Student: Joel Maldonado Rivera
# Student ID: 801-14-3804
# File: ls.py
#
#
#


import sys
import socket

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):
# Contacts the metadata server and ask for list of files.

	created_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # we create the socket

	# We have our try which will try to connect by using the socket which will involve using the ip and port that was passed as a parameter above
	
	try:
	    created_socket.connect((ip,port))
	except socket.error, e:
	    print ("Connection with the server failed. Error: " + str(e))  #error if connection fails
	    sys.exit(1)


	print("Connection to the MetaData Server was successful.")         #if it doesn't fail, the connection was succesful!
	
	# We have this try where it will continue checking till the reply is not equal to NAK.
	# When that occurs it will check if there are any existing files and print them out.
	# If there are no files then it will say so and exit the while if eitehr of these happen.
	try:
	    packet= Packet()
	    reply = "NAK"
	    
	    while reply == "NAK":
	        packet.BuildListPacket()
	        created_socket.sendall(packet.getEncodedPacket())
	        reply = created_socket.recv(1024)
	        if reply != "NAK":
	        	packet.DecodePacket(reply)
	        	file_list = packet.getFileArray()
		    
		    	if not file_list:
		    		print "No files exists here."

		    	else:
		    		for a in file_list:
			    		print (a[0], str(a[1]) + "bytes")
			    
	finally:
		created_socket.close()     #socket that was used is closed
if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server == 2):
		ip = server[0]
		port = int (server[1])

	if not ip:
		usage()

	client(ip, port)