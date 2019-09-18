from __future__ import absolute_import
from collections.abc import Sequence
import logging
import os

import pandas
import synapseclient

from genie.example_filetype_format import FileTypeFormat
from genie import process_functions

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Workflow(FileTypeFormat):

    _fileType = "veoibd_workflow"

    _process_kwargs = ["databaseToSynIdMappingDf"]

    _required_columns = ['assay_id', 'center', 'entity_id']

    def _get_dataframe(self, filePathList):
        if isinstance(filePathList, list):
            filePathList = filePathList[0]

        assay_id = self._get_assay_id(filePathList)
        df = pandas.DataFrame([dict(assay_id=assay_id, center=self.center)])

        return(df)

    def _validateFilename(self, filePath):
        assert os.path.basename(filePath[0]).startswith("VEOIBD-" + self.center + "-ASSAY") and \
               os.path.basename(filePath[0]).endswith(".md")

    def process_steps(self, filePath, databaseToSynIdMappingDf):
        logger.debug("Performing process_steps for {}".format(self._fileType))

        folder_id = databaseToSynIdMappingDf.Id[
            databaseToSynIdMappingDf['Database'] == self._fileType][0]

        table_id = databaseToSynIdMappingDf.Id[
            databaseToSynIdMappingDf['Database'] == f"{self._fileType}_table"][0]

        logger.debug(f"Storing file at {folder_id}")
        f = self.syn.store(synapseclient.File(filePath, parent=folder_id,
                                              annotations=dict(center=self.center, 
                                                           fileType=self._fileType)),
                           forceVersion=False)

        # Add information about assay to the table
        data = self._get_dataframe(filePath)
        data['entity_id'] = f.id
        process_functions.updateData(syn=self.syn, databaseSynId=table_id, 
                                     newData=data, filterBy=self.center,
                                     filterByColumn="center", col=self._required_columns,
                                     toDelete=True)

        return(filePath)

    def _get_assay_id(self, filePathList):
        """Get the assay ID from the file name.
        """
        if isinstance(filePathList, list):
            filePathList = filePathList[0]
        logger.debug(filePathList)
        return os.path.basename(filePathList).split(".md")[0]
