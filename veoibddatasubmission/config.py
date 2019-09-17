"""Configuration.

"""

import genie.config

from . import veoibd_clinical
from . import veoibd_workflow

PROCESS_FILES_LIST = [x for x in genie.config.get_subclasses(genie.config.BASE_CLASS)]

PROCESS_FILES = genie.config.make_format_registry_dict(cls_list=PROCESS_FILES_LIST)
