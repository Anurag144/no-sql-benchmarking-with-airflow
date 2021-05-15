import re
import urllib
from urllib import request
import gzip
import shutil
import os


def downloadDataset():

    print("Downloading relationships dataset started..")
    url1 = "https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz"
    file_name1 = re.split(pattern='/', string=url1)[-1]
    r1 = urllib.request.urlretrieve(url=url1, filename=file_name1)

    print("Downloading relationships done..")

    with gzip.open(file_name1, 'rb') as f_in:
       with open("./dags/soc-pokec-relationships.txt", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print("extracting relationships done..")

    print("Downloading profiles dataset started..")
    url1 = "https://snap.stanford.edu/data/soc-pokec-profiles.txt.gz"
    file_name1 = re.split(pattern='/', string=url1)[-1]
    r1 = urllib.request.urlretrieve(url=url1, filename=file_name1)

    print("Downloading profiles done..")

    with gzip.open(file_name1, 'rb') as f_in:
        with open("./dags/soc-pokec-profiles.txt", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("extracting profiles done..")

    #os.remove("./dags/soc-pokec-profiles.txt.gz")
    #os.remove("./dags/soc-pokec-relationships.txt.gz")

    print("Downloading pokec dataset finished..")

if __name__ == "__main__":

    downloadDataset()
