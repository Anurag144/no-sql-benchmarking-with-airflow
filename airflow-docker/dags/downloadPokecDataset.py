import re
import urllib
from urllib import request
import gzip
import shutil
import os


def downloadDataset():

    if os.path.isfile("/temp/soc-pokec-relationships.txt"):
        print("relations dataset already exists")
    else:
        print("Downloading relationships dataset started..")
        url1 = "http://snap.stanford.edu/data/soc-pokec-relationships.txt.gz"
        file_name1 = re.split(pattern='/', string=url1)[-1]
        file_name1 = "/temp/" + file_name1
        r1 = urllib.request.urlretrieve(url=url1, filename=file_name1)

        path = os.path.abspath("soc-pokec-relationships.txt.gz")
        print(path)

        print("Downloading relationships done..")

        with gzip.open(file_name1, 'rb') as f_in:
           with open("/temp/soc-pokec-relationships.txt", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        print("extracting relationships done..")

    if os.path.isfile("/temp/soc-pokec-profiles.txt"):
        print("profiles dataset already exists")
    else:
        print("Downloading profiles dataset started..")
        url1 = "http://snap.stanford.edu/data/soc-pokec-profiles.txt.gz"
        file_name1 = re.split(pattern='/', string=url1)[-1]
        file_name1 = "/temp/" + file_name1
        r1 = urllib.request.urlretrieve(url=url1, filename=file_name1)

        print("Downloading profiles done..")

        with gzip.open(file_name1, 'rb') as f_in:
            with open("/temp/soc-pokec-profiles.txt", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("extracting profiles done..")

        #os.remove("./dags/soc-pokec-profiles.txt.gz")
        #os.remove("./dags/soc-pokec-relationships.txt.gz")

        print("Downloading pokec dataset finished..")



if __name__ == "__main__":

    downloadDataset()
