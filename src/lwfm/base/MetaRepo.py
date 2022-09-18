import os
import pickle

from pathlib import Path

# This is a quick and dirty implementation of a MetaRepo
# This will store info about files as you put/get, so they
# may be retrieved later, even persisting across sessions.

# The MetaRepo is persisted in the form of a pickle file stored at ~/.lwfm/metarepo
# Note that we should switch from pickle to psycopg at some point for performance

class MetaRepo:
    def Notate(fileRef):
    # Given a fileRef, add it to the MetaRepo
        metaRepo = _getMetaRepo()
        if metaRepo is None:
            print("Could not Notate fileRef")
            return
        metaRepo.append(fileRef)
        _saveMetaRepo(metaRepo)
        
    def Find(fileRef):
    # Given an incomplete fileRef, search through the MetaRepo for matching files
        fileList = []

        metaRepo = _getMetaRepo()
        for file in metaRepo:
            if fileRef.getId() is not None       and file.getId() != fileRef.getId():
                continue
            if fileRef.getName() is not None     and file.getName() != fileRef.getName():
                continue
            if fileRef.getMetadata() is not None and file.getMetadata() != fileRef.getMetadata():
                continue
            fileList.append(file)
        return fileList


#************************************************************************************************************************************

    def _getMetaRepoPath():
        directory = str(Path.home()) + str(Path("/.lwfm/"))
        if not os.path.exists(directory):
            os.mkdir(directory)
        filePath = str(Path(directory + "/metarepo"))
        return filePath

    def _getMetaRepo():
        filePath = _getMetaRepo()
        if not os.path.exists(filePath):
            return []
        
        try:
            metaRepo = pickle.load(filePath)
            return metaRepo
        except Exception as e: # There's a pickle.unpickling exception, but pickle.load can throw any number of different exceptions
            print("Error loading MetaRepo!")
            
    def _saveMetaRepo(metaRepo):
        filePath = _getMetaRepo()
        try:
            pickle.dump(metaRepo, filePath)
        except Exception as e: # There's a pickle.pickling exception, but pickle.load can throw any number of different exceptions
            print("Error saving MetaRepo!")

