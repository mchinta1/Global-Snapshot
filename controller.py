import socket
import bank_pb2
import sys
import random
import time

if len(sys.argv) != 3:
    print("Usage:"+sys.argv[0]+" [balance][file_name]")
    exit(1)
number_of_branches = 0
branch_list = {}
try:
    with open("branches.txt","rb") as f:
        file_contents =  (f.read()).splitlines()
        #print(file_contents)
        number_of_branches = len(file_contents)
        for x in file_contents:
            x = x.decode('utf-8')
            x = x.split(' ')
            #print(x)
            branch_list[x[0]] = {"ip":x[1],"port":int(x[2])}
        #print(branch_list)
        #print(branch_list["branch1"]["port"])
except IOError:
    print("Error in file reading")
total_balance = int(sys.argv[1])
if total_balance == 0:
    exit(-1)
branch_share = int(total_balance/number_of_branches)
init = bank_pb2.InitBranch()
br_msg = bank_pb2.BranchMessage()

for branch in branch_list:
    init.balance = branch_share
    br = init.all_branches.add()
    br.ip = branch_list[branch]["ip"]
    br.name = branch
    br.port = branch_list[branch]["port"]
for branch in branch_list:
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect((branch_list[branch]["ip"], branch_list[branch]["port"]))
    #br_msg.init_branch = bank_pb2.InitBranch()
    br_msg.init_branch.CopyFrom(init)
    #br_msg.transfer.money = 100
    req = br_msg.SerializeToString()
    clientsocket.send(req)
    #print("sent!")
    clientsocket.close()

print('Triggering Snap......')
time.sleep(10)
snapshotId = 0
while True:
    sleep_time = random.uniform(0,10)
    br_name = random.choice(list(branch_list.keys()))
    branch_msg = bank_pb2.BranchMessage()
    branch_msg.init_snapshot.snapshot_id = snapshotId
    clientsocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    clientsocket.connect((branch_list[br_name]["ip"],branch_list[br_name]["port"]))
    clientsocket.send(branch_msg.SerializeToString())
    print("sent initsnap to :{0}".format(br_name))
    time.sleep(15)
    branch_msg = bank_pb2.BranchMessage()
    branch_msg.retrieve_snapshot.snapshot_id = snapshotId
    clientsocket.close()
    print("snapshot_id: {}".format(snapshotId))
    for branch in branch_list:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((branch_list[branch]["ip"], branch_list[branch]["port"]))
        clientsocket.send(branch_msg.SerializeToString())
    #print("Retrive sent")
        res = bank_pb2.BranchMessage()
        res.ParseFromString(clientsocket.recv(8000))
        print("{0}:{1},{2}".format(branch,res.return_snapshot.local_snapshot.balance,res.return_snapshot.local_snapshot.channel_state))
    #print(res)
        clientsocket.close()
        time.sleep(sleep_time)
    snapshotId += 1

