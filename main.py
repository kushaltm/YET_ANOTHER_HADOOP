import argparse
from commands import *
parser = argparse.ArgumentParser(
    description='Command line interface to perform operations on YAH')
parser.add_argument('command', choices=['put', 'cat', 'ls', 'rm', 'mkdir', 'rmdir','mapreduce'],help='Commands to choose')
parser.add_argument('--argument1','-arg1',help = 'input file for put ,path for cat,rm,mkdir,ls')
parser.add_argument('--argument2','-arg2',help = 'destination for put ')
parser.add_argument('--input','-i',help = 'input file for mapreduce job')
parser.add_argument('--ouput','-o',help = 'output file for mapreduce job')
parser.add_argument('--config','-c',help = 'configuration file for mapreduce')
parser.add_argument('--mapper','-m')
parser.add_argument('--reducer','-r')

args = parser.parse_args()
command = args.command

if command == 'put':
    source = args.argument1
    destination = args.argument2
    put(source,destination)

elif command = 'cat':
    path = args.argument1
    cat(path)

elif command = 'ls':
    path = args.argument1
    ls(path)

elif command = 'rm':
    path = args.argument1
    remove(path)

elif command = 'mkdir':
    path = args.argument1
    make_dir(path)

elif command = 'rmdir':
    path = args.argument1
    remove_dir(path)

elif command = 'mapreduce':
    fs_input = args.input
    fs_output = args.output
    config_path = args.config
    abs_mapper = args.mapper
    abs_reducer = args.reducer
    mapreduce(fs_input,fs_output,config_path,abs_mapper,abs_reducer) 



