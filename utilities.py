
import os
import json
from math import ceil


def fileSplit(path, splitSize):
	file = open(path, 'r')
	size = os.path.getsize(path)

	blocks = ceil(size / splitSize)

	for _ in range(blocks):
		yield file.read(splitSize)



def updateJSON(data, file):
	file.seek(0)#poinst to beginning
	file.truncate(0)#method resizes the file to the given number of bytes.
	json.dump(data, file, indent=4)#indent parameter allows us to format the JSON array elements and object members in a more organized manner.
	file.close()
