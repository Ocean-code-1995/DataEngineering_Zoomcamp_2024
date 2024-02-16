# you have a Dockerfile and a Python script (pipeline.py). The Dockerfile is used to build a Docker image, and the Python script is executed inside a Docker container created from that image

import pandas as pd
import sys
import argparse # for command-line arguments

# list (command line) arguments
print(sys.argv)
#print(sys.argv[0]) # prints script name

#assigns the first command-line argument (after the script name) to the variable day.
day = sys.argv[1]

print("Hello World! Today is " + day)