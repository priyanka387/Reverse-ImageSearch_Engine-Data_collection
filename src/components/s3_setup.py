import os
import sys
from zipfile import ZipFile
import shutil
from src.exception import CustomException




class DataStore:
    """this class create a datastore where rootpath is joined to data folder and then it is joined to archive folder & images"""
    def __init__(self):
        self.root = os.path.join(os.getcwd(), "data")
        self.zip = os.path.join(self.root, "archive.zip")
        self.images = os.path.join(self.root, "caltech-101")
        self.list_unwanted = ["BACKGROUND_Google"]

    def prepare_data(self):
        """this method just unzip the folder and extract all the data and print process is completed"""
        try:
            print(" Extracting Data ")
            with ZipFile(self.zip, 'r') as files:
                files.extractall(path=self.root)

            files.close()
            print(" Process Completed ")
        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}

    def remove_unwanted_classes(self):
        """this class removes unwanted image classes"""
        try:
            print(" Removing unwanted classes ")
            for label in self.list_unwanted:
                path = os.path.join(self.images,label)
                shutil.rmtree(path, ignore_errors=True)
            print(" Process Completed ")
        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}

    def sync_data(self):
        """this method sync the images to the s3 bucket"""
        try:
            print("\n====================== Starting Data sync ==============================\n")
            os.system(f"aws s3 sync { self.images } s3://image-search-engine-data-collection-new/images/ ")
            print("\n====================== Data sync Completed ==========================\n")

        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}

    def run_step(self):
        """this method runs all the methods created above """
        try:
            self.prepare_data()
            self.remove_unwanted_classes()
            self.sync_data()
            return True
        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}


if __name__ == "__main__":
    store = DataStore()
    store.run_step()
