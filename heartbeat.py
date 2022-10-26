import time 
import os
import utilities import *
import shutil
import shutil

config_file = open('current_config.json','r')
config = json.dump(config_file)

datanode = os.path.expandvars(config['path_to_datanode'])
namenode = os.path.expandvars(config['path_to_namenode'])
no_of_nodes = os.path.expandvars(config['num_datanodes'])
datanode_size = os.path.expandvars(config['datanode_size'])
sync_period = config['sync_period']


def namenode_heartbeat():
    while(True):
        #check for namenode failure
        try:
            location_file = open(namenode+'location_file.json','r')
            location_data=  json.load(location_data)
            break
        except:
            pass 
    #handle datanode failure

    for dir_no in range(1,no_of_nodes+1):
        if not os.path.isdir(datanode+"Datanodes/DN"+str(dir_no)):
            os.mkdir(datanode+"Datanodes/DN"+str(dir_no))
            print("Datanode DN",str(dir_no)," restored")
        
    for file in location_data.keys():
        all_blocks = location_data[file]
        for replicas in all_blocks:
            for i in range (0,len(replicas)):
                if not os.path.isfile(datanode+"Datanode/"+replicas[i]):
                    print("Block doesn't exist  recreating ",replicas[i])
                    if(i==0):
                        while(not os.path.isfile(datanode+"Datanodes/"+replicas[i])):
                            i+=1
                        t = 0
                        while t!=i:
                            shutil.copy(datanode+"DataNodes/"+replicas[i],datanodes+"DataNodes/"+replicas[t])
                            t+=1
                    else:
                        shutil.copy(datanode+"DataNodes/"+replicas[i-1],datanode+"DataNodes/"+replicas[i])
    print("everything OK")

while(True):
    time.sleep(sync_period)
    namenode_heartbeat()                    