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
        self.snapshots = {}
        self.sendMarker = {}
        self.incomingRecord = {}
        print("Branch initialized")

    def initBranch(self,init):
        self.branches_list = {}
        self.mutex.acquire()
        self.intialBalance = init.balance
        self.lower = int(0.01 * self.intialBalance)
        self.upper = int(0.05 * self.intialBalance)
        self.balance = init.balance
        self.mutex.release()
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
            for x in self.incomingRecord:
                if (self.incomingRecord[x]):
                    self.snapshots[x]["incoming"].append(amount)
            self.mutex.release()

    def InitSnapshot(self,snapshotId):
        global snapId
        snapId = snapshotId
        #record local state
        self.recordLocalState(snapId)
        #record in coming channel
        #Send Markers
        self.mutex.acquire()
        self.incomingRecord[snapId] = True
        self.sendMarker[snapId] = True
        self.mutex.release()

    def recordLocalState(self,snapId):
        self.mutex.acquire()
        self.snapshots[snapId] = {"localState":self.balance,"incoming":[]}
        #print("Snapshot :{0}".format(self.snapshots))
        self.mutex.release()

    def sendMarkerMsg(self,snapId):
        b = bank_pb2.BranchMessage()
        b.marker.snapshot_id = snapId
        return b

    def marker(self,markerMsg):
        print("Received Marker !")
        if markerMsg.snapshot_id not in self.snapshots:
            self.mutex.acquire()
            #record local state
            self.snapshots[markerMsg.snapshot_id] = {"localState":self.balance, "incoming":[]}
            #send markers
            self.sendMarker[snapId] = True
            #start recording of incoming
            self.incomingRecord[markerMsg.snapshot_id] = True
            self.mutex.release()
            #print(self.snapshots)
        elif  self.incomingRecord[markerMsg.snapshot_id]:
            self.mutex.acquire()
            self.incomingRecord[markerMsg.snapshot_id] = False
            self.mutex.release()
            print("****Snap {1} Captured*** : {0}".format(self.snapshots[markerMsg.snapshot_id],markerMsg.snapshot_id))


    def Server(self,sock):
        while True:
            sock.listen(10)
            print("Server Thread Started.....!")
            con, ip = sock.accept()
            msg = bank_pb2.BranchMessage()
            msg.ParseFromString(con.recv(8000))

            if (msg.HasField("transfer")):
                self.transfer(msg.transfer.money)
            elif (msg.HasField("init_snapshot")):
                print("Init Snap: {0}".format(msg.init_snapshot))
                self.InitSnapshot(msg.init_snapshot.snapshot_id)
            elif(msg.HasField("marker")):
                self.marker(msg.marker)
            elif(msg.HasField("retrieve_snapshot")):
                ret = bank_pb2.BranchMessage()
                ret.return_snapshot.local_snapshot.snapshot_id = msg.retrieve_snapshot.snapshot_id
                self.mutex.acquire()
                if msg.retrieve_snapshot.snapshot_id not in self.snapshots:
                    self.snapshots[msg.retrieve_snapshot.snapshot_id] = {"localState":self.balance,"incoming":[]}
                ret.return_snapshot.local_snapshot.balance = self.snapshots[msg.retrieve_snapshot.snapshot_id]["localState"]
                # for x in self.snapshots[msg.retrieve_snapshot.snapshot_id]["incoming"]:
                #     ch_state = ret.return_snapshot.local_snapshot.channel_state.add()
                #     ch_state = x
                ret.return_snapshot.local_snapshot.channel_state.extend(self.snapshots[msg.retrieve_snapshot.snapshot_id]["incoming"])
                con.send(ret.SerializeToString())
                self.mutex.release()


    #
    def initTransfer(self,sock):
        global snapId
        while True:
            #global br_list
            for x in self.sendMarker:
                if (self.sendMarker[x]):
                    b = self.sendMarkerMsg(x)
                    for branch in self.branches_list:
                        sock.connect((self.branches_list[branch]["ip"],self.branches_list[branch]["port"]))
                        sock.send(b.SerializeToString())
                        sock.close()
                        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    self.mutex.acquire()
                    self.sendMarker[x] = False
                    self.mutex.release()

            print("inint Transfer")
            # for key in br_list:
            if (len(self.branches_list) == 0):
                break
            key = random.choice(list(self.branches_list.keys()))
            sock.connect((self.branches_list[key]["ip"], self.branches_list[key]["port"]))
            msg = bank_pb2.BranchMessage()
            print("Transferring to - {0} on port: {1}".format(key, self.branches_list[key]["port"]))
            amnt = random.randint(self.lower,self.upper)
            msg.transfer.money = amnt
            self.mutex.acquire()
            print("Before Balance: {0} ".format(self.balance))
            self.balance -= msg.transfer.money
            print("Current Balance: {0} ".format(self.balance))
            self.mutex.release()
            req = msg.SerializeToString()
            sock.send(req)
            # print("sent!")
            sock.close()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            time.sleep(5)



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
snapId = 0



#
# threading.Thread(target=Server, args=(listen_socket,)).start()
# threading.Thread(target=initTrasnfer, args=(client_socket,)).start()


class ServerThread (threading.Thread):
   def __init__(self, sock):
      threading.Thread.__init__(self)
      self.sock =sock

   def run(self):
       branch.Server(self.sock)

class clientThread (threading.Thread):
   def __init__(self, sock):
      threading.Thread.__init__(self)
      self.sock =sock

   def run(self):
      print("Starting Client")
      branch.initTransfer(self.sock)

t1 = ServerThread(listen_socket)
t2 = clientThread(client_socket)
t1.start()
t2.start()
