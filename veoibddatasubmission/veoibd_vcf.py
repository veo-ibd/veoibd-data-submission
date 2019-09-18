from collections.abc import Sequence
import datetime
import io
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

class Vcf(FileTypeFormat):
    """This is the definition for a single sample VCF.

    """

    # This should match what is in the database mapping table
    _fileType = "vcf"

    # The filename must match this regular expression
    _required_filename = "^VEOIBD-.+-.+-.+\.vcf$"

    # The sample ID must match this regular expression
    _required_id_format = "^VEOIBD-.+-.+-.+$"

    _process_kwargs = [
        "newPath", "parentId", "databaseToSynIdMappingDf"]

    def _validateFilename(self, filePath):

        if isinstance(filePath, list):
            filePath = filePath[0]
        
        compiled_re = re.compile(self._required_filename)
        if compiled_re.fullmatch(os.path.basename(filePath)):
            logger.debug("{} filename is validated.".format(self._fileType))
        else:
            logger.debug("{} filename is not valid: {}.".format(self._fileType, filePath))
            raise AssertionError("{} filename ({}) is not correct.".format(self._fileType,
                                                                           os.path.basename(filePath))


    def _get_dataframe(self, filePathList):
        headers = None

        if isinstance(filePath, list):
            filepath = filePathList[0]

        with open(filepath, "r") as vcffile:
            lines = [l for l in vcffile if not l.startswith('##')]
        
        if not lines[0].startswith("#CHROM"):
            raise ValueError("Your vcf must start with the header #CHROM")

        sample_id = os.path.basename(filePath).rstrip(".vcf")
        compiled_re = re.compile(f"^{sample_id}$")

        if not compiled_re.fullmatch(lines[0]):
            raise ValueError("Your vcf does not contain a column for the sample named by the file.")

        lines[0] = lines[0].lstrip("#")
        vcf = pd.read_csv(io.StringIO(''.join(lines)),
                          sep="\t", names=headers)
        else:
        return(vcf)

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
