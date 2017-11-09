import sys
import bank_pb2
import socket
import threading
import os
import time
import _thread
import random
class branch:
    def __init__(self,ip,port,name):
        self.ip = ip
        self.port = port
        self.name = name
        self.balance = 0
        self.mutex = threading.Lock()
        print("Branch initialized")

    def initBranch(self,init):
        self.branches_list = {}
        self.balance = init.balance
        self.number_of_branches = len(init.all_branches)
        for branch in init.all_branches:
            if(branch.name == self.name):
                    self.ip = branch.ip
                    continue
            self.branches_list[branch.name] = {"ip":branch.ip,"port":branch.port}
        #print("Account initialized")
        print("Account-Balance:{0} ".format(self.balance))
        #print("Self Branch -> name:{0}  ip:{1} port: {2} ".format(self.name,self.ip,self.port))
        #print("branches-list {0} ".format(self.branches_list))

    def transfer(self,amount):
        if amount > 0:
            self.mutex.acquire()
            self.balance += amount
            print("balance added to{0}".format(self.name))
            self.mutex.release()

if len(sys.argv) != 3:
    print("Usage:", sys.argv[0],"[BRANCH_NAME] [PORT]")
    sys.exit(-1)
port = int(sys.argv[2])
branch_name = sys.argv[1]
branch = branch('localhost',port,branch_name)
init = bank_pb2.InitBranch()
br_msg = bank_pb2.BranchMessage()
listen_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
listen_socket.bind(("",port))
listen_socket.listen(2)
con,ip = listen_socket.accept()
br_msg.ParseFromString(con.recv(8000))
branch.initBranch(br_msg.init_branch)
#print(branch.number_of_branches)
br_list = branch.branches_list

client_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)


def Server(sock):
    while True:
        sock.listen(10)
        print("Server Thread Started.....!")
        con,ip = sock.accept()
        msg = bank_pb2.BranchMessage()
        msg.ParseFromString(con.recv(8000))

        if (msg.HasField("transfer")):
            branch.transfer(msg.transfer.money)
        elif (msg.HasField("init_snapshot")):
            print("Init Snap: {0}".format(msg.init_snapshot))

#
def initTrasnfer(sock):
    while True:
            print("inint Transfer")
        #for key in br_list:
            if(len(br_list)==0):
                break
            key = random.choice(list(br_list.keys()))
            sock.connect((br_list[key]["ip"],br_list[key]["port"]))
            msg = bank_pb2.BranchMessage()
            print("Transferring to - {0} on port: {1}".format(key, br_list[key]["port"]))

            msg.transfer.money = 10
            print("Before Balance: {0} ".format(branch.balance))
            branch.balance -= msg.transfer.money
            print("Current Balance: {0} ".format(branch.balance))

            req=msg.SerializeToString()
            sock.send(req)
            #print("sent!")
            sock.close()
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            time.sleep(5)

#
# threading.Thread(target=Server, args=(listen_socket,)).start()
# threading.Thread(target=initTrasnfer, args=(client_socket,)).start()


class ServerThread (threading.Thread):
   def __init__(self, sock):
      threading.Thread.__init__(self)
      self.sock =sock

   def run(self):
      Server(self.sock)

class clientThread (threading.Thread):
   def __init__(self, sock):
      threading.Thread.__init__(self)
      self.sock =sock

   def run(self):
      print("Starting Client")
      initTrasnfer(self.sock)

t1 = ServerThread(listen_socket)
t2 = clientThread(client_socket)
t1.start()
t2.start()
