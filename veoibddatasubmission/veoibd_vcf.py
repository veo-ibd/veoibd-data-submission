from collections.abc import Sequence
import datetime
import io
import json
import logging
import os
import re
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
    _fileType = "veoibd_vcf"

    # The filename must match this regular expression
    _required_filename = r"^VEOIBD-.+-.+-.+\.vcf$"

    # The sample ID must match this regular expression
    _required_id_format = r"^VEOIBD-.+-.+-.+$"

    _process_kwargs = ["databaseToSynIdMappingDf"]

    _required_columns = ["CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]

    def _validateFilename(self, filePath):

        if isinstance(filePath, list):
            filePath = filePath[0]
        
        compiled_re = re.compile(self._required_filename)
        if compiled_re.fullmatch(os.path.basename(filePath)):
            logger.debug("{} filename is validated.".format(self._fileType))
        else:
            err_msg = "{} filename is not valid: {}.".format(self._fileType, os.path.basename(filePath))
            logger.debug(err_msg)
            raise AssertionError(err_msg)


    def _get_dataframe(self, filePathList):
        headers = None

        if isinstance(filePathList, list):
            filepath = filePathList[0]
        else:
            filepath = filePathList

        with open(filepath, "r") as vcffile:
            lines = [l for l in vcffile if not l.startswith('##')]
        
        if not lines[0].startswith("#CHROM"):
            raise ValueError("Your vcf must start with the header #CHROM")

        lines[0] = lines[0].lstrip("#")
        vcf = pd.read_csv(io.StringIO(''.join(lines)),
                        sep="\t", names=headers)
        return(vcf)

    def process_steps(self, filePath, databaseToSynIdMappingDf):

        data = self._get_dataframe(filePath)
        logger.debug("Performing process_steps for {}".format(self._fileType))

        folder_id = databaseToSynIdMappingDf.Id[
            databaseToSynIdMappingDf['Database'] == self._fileType][0]

        specimen_id = os.path.basename(filePath).rstrip(".vcf")
        logger.debug(f"Storing file at {folder_id}")
        f = self.syn.store(synapseclient.File(filePath, parent=folder_id,
                                              annotations=dict(center=self.center,
                                                               specimenID=specimen_id, 
                                                               fileType=self._fileType)),
                           forceVersion=False)

        return(filePath)

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

        compiled_re = re.compile(self._required_id_format)
        for required_column in self._required_columns:
            if required_column not in data.columns:
                total_error += f"Your vcf does not contain a {required_column} column.\n"

        sample_columns = []
        for column in data.columns:
            if compiled_re.fullmatch(column):
                sample_columns.append(column)
        
        if not sample_columns:
            total_error += "Your vcf does not contain a column for the sample named by the file.\n"

        if len(sample_columns) > 1:
            total_error += "Your vcf contains more than one a sample column.\n"

        return(total_error, warning)
