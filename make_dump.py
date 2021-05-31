import glob
import os
import gzip

path = os.path.dirname(os.path.realpath(__file__))
os.chdir(path)

if os.path.exists("./sql/dump.sql"):
    os.remove("./sql/dump.sql")

read_files = glob.glob("./sql/public/*.sql")
read_files.insert(0, "./sql/create.sql")

print(read_files)

with open("./sql/dump.sql", "wb") as outfile:
    for f in read_files:
        i = 0
        line = "\n\n"
        i += 1
        outfile.write(line.encode('utf-8'))
        with open(f, "rb") as infile:
            outfile.write(infile.read())