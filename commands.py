import datetime 
import os
import json
from utilities import *
import subprocess

config_file = open('current_config.json','r')
config = json.load(config_file)
no_of_nodes = config['num_datanodes']
replication = config['replication_factor']
block_size = config['block_size']# in bytes

datanode = os.path.expandvars(config['path_to_datanodes'])#os.path.expandvars() method in Python is used to expand the environment variables in the given path like HOME will be user
namenode = os.path.expandvars(config['path_to_namenodes'])

if not os.path.isdir(namenode):
    raise Exception("Please wait few seconds namenode is intialising or load DFS first")


secondary_namenode_path = os.path.expandvars(config['secondary_namenode_path'])
fs_path = os.path.expandvars(config['fs_path'])#yet to explore
datanode_log_path = os.path.expandvars(config['datanode_log_path'])

name_node_logfile_path = os.path.expandvars(config['namenode_log_path'])
namenode_log_file = open(name_node_logfile_path,'a+')

datanode_path = os.path.join(datanode,'DataNodes')

def put(source,destination):# source is the local folder destination is the hdfs folder
    user_file = source.split('/')[-1]#file name

    dest_path = os.path.join(destination,user_file)#creates the destination path with filename


    #mapping the virtual path and file name
    mapping_file = open(namenode+'mapping_file.json','r+')#loaded when setup if runned 
    mapping_data = json.load(mapping_data)# in namenode stores the directory and files in it

    
    if not destination in mapping_data:#checks if destination path exists
        mapping_File.close()
        namenode_log_file.write(str(datatime.datetime.now())+" path error "+destination+" does not exist\n")
        raise Exception(destination,"No such directory or create directory if not created")
    
    if user_file in mapping_data[destination]:
        raise Exception("file already exists")
    
    mapping_data[destination].append(user_file)
    updateJSON(mapping_data,mapping_File)#updates mapping file with new data added
    namenode_log_file.write(str(datetime.datetime.now()) + " : mapping file updated "+str(os.path.getsize(namenode+"mapping_file.json")) +" bytes"+"\n")

    #file to keep track of where file blocks are stored
	location_file = open(namenode + "location_file.json","r+")#stores the location of block with datanode 
	location_data = json.load(location_file)
	location_data[dest_path] = []#creates the new location

	datanode_tracker = open(namenode + 'datanode_tracker.json', 'r+')#tracks which datanodes are empty and filled
	datanode_details = json.load(datanode_tracker)

    #splitting files to datanodes
	next_datanode = datanode_details['Next_datanode']#to fill the block
    for split in fileSplit(source,block_size):#data is split based on the block size and yielded here
        replica = []
        for _ in range(replication):
            empty_blk_no = 0
            cur_no = 0
            while cur_no < no_of_nodes:#no_of_nodes  is the no_of_datanodes
                DN_str = 'DN'+str(next_datanode)
                try:
                    empty_blk_no = datanode_details[DN_str].index(0)
                    break
                except Exception:
                    cur_no+=1
                    next_datanode = (next_datanode%no_of_nodes)+1
            if cur_no == no_of_nodes:
                raise Exception("All Datanodes are full")
            block = "block"+str(empty_blk_no)
            datanode_details[DN_str][empty_blk_no] = 1
            next_datanode = (next_datanode % no_of_nodes) + 1#after filling th datanode move to next node

            store_path = DN_str+'/'+block
            replica.append(store_path)

            logpath = os.path.join(datanode_log_path,DN_str)
			logpath = logpath + ".txt"
			datanode_log = open(logpath,'a+')
			datanode_log.write(str(datetime.datetime.now()) + " : block allocated " + str(block) +" \n")
			datanode_log.close()

            file_path = os.path.join(datanode_path, store_path)
			file = open(file_path,'w')
			file.write(split)
			file.close()
        location_data[dest_path].append(replica)
        # end of storing data is datanode with replication factor
    namenode_log_file.write(str(datetime.datetime.now()) + " : A new file is added ->" + user_file +"\n")
	datanode_details['Next_datanode'] = next_datanode

    updateJSON(datanode_details, datanode_tracker)
    namenode_log_file.write(str(datetime.datetime.now()) + " : datanode_tracker file updated "+ str(os.path.getsize(namenode + "datanode_tracker.json"))+" bytes"+"\n")
	updateJSON(location_data, location_file)
	namenode_log_file.write(str(datetime.datetime.now()) + " : location file updated "+ str(os.path.getsize(namenode + "location_file.json"))+" bytes"+"\n")

def cat(path):#displays the content of the file
    location_file = open(namenode + "location_file.json","r+")
    location_data = json.load(location_file)
    
    if not path in location_data:
        location_file.close()
        raise Exception(path,"File does not exist")

    for replica in location_data[path]:
        for file_blk in replica:
            block_path = os.path.join(datanode,"Datanode",file_blk)
            if os.path.isfile(block_path):
                content = open(block_path,'r').read()
                print(content,end ='')
                break # one file_blk represents the one datablock out of many split

def ls_command(path):
	mapping_file = open(namenode + "mapping_file.json",'r')
	mapping_data = json.load(mapping_file)
	
	#directory doesn't exist
	if not path in mapping_data:
		mapping_file.close()
		raise Exception(path,"No such directory")

	for entry in mapping_data[path]:
		if (path + entry) in mapping_data:
			print(entry, "\tDirectory")
			
		else:
			print(entry, "\tfile")

	mapping_file.close()

def rmdir_command(path):
	split = os.path.split(path)
	par_path, user_dir = split[0], split[1]#split[0] points to first half or head split[1] to other half

	mapping_file = open(namenode + "mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)
	
	#directory doesn't exist
	if not path in mapping_data:
		mapping_file.close()
		raise Exception(path,"No such directory")

	if len(mapping_data[path]) != 0:								#check for any file/dir in path
		mapping_file.close()
		raise Exception(path,"Directory is not empty")

	del mapping_data[path]
	
	#deleting entry in parent directory
	if par_path in mapping_data:
		mapping_data[par_path].remove(user_dir)


	updateJSON(mapping_data, mapping_file)       

def mkdir_command(path):
	split = os.path.split(path)
	par_path, user_dir = split[0], split[1]

	mapping_file = open(namenode + "mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)
	
	#have to create parent directory before creating subdirectory
	if not par_path in mapping_data:
		mapping_file.close()
		raise Exception(par_path,"No such directory")

	#appends subdirectory to parent directory
	mapping_data[par_path].append(user_dir)
	mapping_data[path] = []

	updateJSON(mapping_data, mapping_file)

def rm_command(path):
	split = os.path.split(path)
	par_path, file = split[0], split[1]

	location_file = open(namenode + "location_file.json",'r+')
	location_data = json.load(location_file)

	if not path in location_data:
		location_file.close()
		raise Exception(path, "File does not exist")


	datanode_tracker = open(namenode + 'datanode_tracker.json', 'r+')
	datanode_details = json.load(datanode_tracker)

	for replica in location_data[path]:
		for file_blk in replica:
			DN_str, block = file_blk.split('/')
			blocknum = int(block[5:])#getting the block number

			datanode_details[DN_str][blocknum] = 0

			block_path = os.path.join(datanode, 'DataNodes', file_blk)
			os.remove(block_path)

			logpath = os.path.join(datanode_log_path,DN_str)
			logpath = logpath + ".txt"
			datanode_log = open(logpath,'a+')
			datanode_log.write(str(datetime.datetime.now()) + " : block removed " + str(block) +" \n")
			datanode_log.close()

	del location_data[path]#removes the whole file location 
	namenode_log_file.write(str(datetime.datetime.now()) + " : file is removed ->" + file +"\n")

	mapping_file = open(namenode + "mapping_file.json",'r+')
	mapping_data = json.load(mapping_file)
	mapping_data[par_path].remove(file)

	updateJSON(mapping_data, mapping_file)
	namenode_log_file.write(str(datetime.datetime.now()) + " : mapping file updated "+str(os.path.getsize(namenode+"mapping_file.json")) +" bytes"+"\n")
	updateJSON(location_data, location_file)
	namenode_log_file.write(str(datetime.datetime.now()) + " : location file updated "+ str(os.path.getsize(namenode + "location_file.json"))+" bytes"+"\n")
	updateJSON(datanode_details, datanode_tracker)
	namenode_log_file.write(str(datetime.datetime.now()) + " : datanode_tracker file updated "+ str(os.path.getsize(namenode + "datanode_tracker.json"))+" bytes"+"\n")

def mapreduce(fs_input,fs_output,config_path,abs_mapper,abs_reducer):
    mapping_file = open(namenode+"mapping_file.json",'r')
    mapping_data = json.load(mapping_file)
    if not fs_input in mapping_data or not fs_output in mapping_data:
	 	raise Exception("Input or Output directory doesn't exist")
    p1 = subprocess.Popen('python3 main.py cat -arg1 {} | python3 {} | sort-k1,1 | python3 {} > t.txt'.format(fs_input,abs_mapper,abs_reducer),stdin = subprocess.PIPE, shell = True)
    os_path = os.getcwd()+"/t.txt"
    p1.wait()# wait untill process completes
    subprocess.Popen('python3 main put -arg1 {} -arg2 {}'.format(op_path,fs_output),shell = True)
    

    """
    location_file example
    {
    "/input/utilities.py": [
        [
            "DN5/block0",
            "DN1/block0",
            "DN2/block0"
        ],
        [
            "DN3/block0",
            "DN4/block0",
            "DN5/block1"
        ],
        [
            "DN1/block1",
            "DN2/block1",
            "DN3/block1"
        ]
    ]
}"""

"""
mapping_file.json
{
    "/": [
        "input"
    ],
    "/input": [
        "utilities.py"
    ]
}"""