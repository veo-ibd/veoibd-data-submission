from collections.abc import Sequence
import datetime
import json
import logging
import os
import tempfile
import urllib

import jsonschema
import jsonref
import pandas as pd
import synapseclient

from genie.example_filetype_format import FileTypeFormat
from genie import process_functions

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Clinical(FileTypeFormat):

    # This should match what is in the database mapping table
    _fileType = "clinical_filetype"

    _schema_url = None

    _required_filename = "clinical_filetype.csv"

    _process_kwargs = [
        "newPath", "parentId", "databaseToSynIdMappingDf"]
    
    # This should match what is in the table that the data goes into
    _required_columns = ["col1", "col2", "center"]

    # This should match what the the primary key is set as an annotation
    # on the table the data goes into.
    _primary_key_columns = ["primary_key_col"]


    def _validateFilename(self, filePath):

        if isinstance(filePath, list):
            filePath = filePath[0]
        
        if os.path.basename(filePath) == self._required_filename:
            logger.debug("{} filename is validated.".format(self._fileType))
        else:
            logger.debug("{} filename is not valid: {}.".format(self._fileType, filePath))
            raise AssertionError("{} filename ({}) is not correct. It should be {}".format(self._fileType,
                                                                                           os.path.basename(filePath),
                                                                                           self._required_filename))


    def _get_dataframe(self, filePathList):
        if isinstance(filePathList, list):
            filePathList = filePathList[0]

        df = pd.read_csv(filePathList, 
                         true_values=["TRUE", "true", "True"],
                         false_values=["FALSE", "false", "False"])
        df = df.fillna("")

        return(df)


    def process_steps(self, data, databaseToSynIdMappingDf, 
                      newPath, parentId):
        table_id = databaseToSynIdMappingDf.Id[
            databaseToSynIdMappingDf['Database'] == self._fileType][0]
        
        data['center'] = self.center

        logger.debug(f"Updating {self._fileType} data in table {table_id}.")
        process_functions.updateData(syn=self.syn, databaseSynId=table_id, 
                                     newData=data, filterBy=self.center,
                                     filterByColumn="center", col=None,
                                     toDelete=True)
        
        data.to_csv(newPath, sep="\t", index=False)
        return(newPath)


    def _validate(self, data):
        """
        This function validates the clinical file to make sure it adheres
        to the clinical SOP.

        Args:
            data: Pandas data frame with individual metadata

        Returns:
            Error and warning messages
        """
        total_error = ""
        warning = ""

        data_dicts = data.to_dict(orient="records")

        clean_data_dicts = []

        for row in data_dicts:
            for k in row: 
                if row[k] in ['TRUE', 'FALSE']: 
                    row[k] = True if row[k] == 'TRUE' else False
                if row[k] in ['']:
                    row[k] = None
            clean_row = {k: v for k, v in row.items() if v is not None}
            clean_data_dicts.append(clean_row)

        schema = jsonref.load(urllib.request.urlopen(self._schema_url))
        val = jsonschema.Draft7Validator(schema=schema)
        for n, record in enumerate(clean_data_dicts): 
            i = val.iter_errors(record) 
            for error in i: 
                total_error += f"Row {n} had the following error in column '{error.path}': {error.message}\n"

        # Check if the count of the primary key is not distinct
        res = data.groupby(self._primary_key_columns).size()
        if (res > 1).any():
            total_error += "Found duplicated {primaryKey}'s in the file.".format(primaryKey=self._primary_key_columns)

        return(total_error, warning)

class ClinicalIndividual(Clinical):

    _fileType = "veoibd_clinical_individual"
    _schema_url = "https://raw.githubusercontent.com/kdaily/JSON-validation-schemas/master/validation_schemas/VEOIBD/veoibd_metadata_schema.json"

    _required_filename = "clinical_individual.csv"
    
    _primary_key_columns = ["individualID"]


class ClinicalSpecimen(Clinical):

    _fileType = "veoibd_clinical_specimen"
    _schema_url = "https://raw.githubusercontent.com/kdaily/JSON-validation-schemas/master/validation_schemas/VEOIBD/veoibd_specimen_schema.json"

    _required_filename = "clinical_specimen.csv"

    _primary_key_columns = ["specimenID"]
