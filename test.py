import socket
import bank_pb2

clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
b = bank_pb2.BranchMessage()
b.transfer.money = 100
clientsocket.connect(("",8080))
req = b.SerializeToString()
clientsocket.send(req)