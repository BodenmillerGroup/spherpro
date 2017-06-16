import yaml
import collections
import copy

"""
This module defines the configuration file structure,
defaults, required files, loading as well as validation functions.
"""



"""
List of keywords fields used in the configuration file
"""
BACKEND = 'backend'
BARCODE_CSV = 'barcode_csv'
CONDITION = 'condition_col'
ID = 'id_col'
LAYOUT_CSV = 'layout_csv'
MODNAME = 'modname_col'
MODPRE = 'modpre_col'
NAME = 'name_col'
PARENT = 'parent_col'
PATH = 'path'
PLATE = 'plate_col'
REF = 'ref_col'
SEP = 'sep'
STACK = 'stack_col'
STACK_DIR = 'stack_dir'
STACK_RELATIONS = 'stack_relations'
TYPE = 'type_col'
WELL_COL = 'well_col'
CPOUTPUT = 'cpoutput'
MEASUREMENT_CSV = 'measurement_csv'
IMAGES_CSV = 'images_csv'
RELATION_CSV = 'relation_csv'
CP_DIR = 'cp_dir'
PANNEL_CSV = 'pannel_csv'
CHANNEL_NAME = 'channel_name_col'
DISPLAY_NAME = 'display_name_col'
PANNEL_ID = 'pannel_id'
CHANNEL_TYPE = 'channel_type'
CHANNEL_TYPE_DEFAULT = 'IMC'
OBJECTS = 'objects'
FILETYPE = 'filetype'
MASKFILENAME_PEFIX = 'mask_filename_col_prefix'
RELATIONSHIP = 'relationship'
OBJECTID_FROM = 'first_object_id_col'
OBJECTID_TO = 'second_object_id_col'
OBJECTNUMBER_FROM = 'first_object_number_col'
OBJECTNUMBER_TO = 'second_object_number_col'
IMAGENUMBER_FROM = 'first_image_number_col'
IMAGENUMBER_TO = 'second_image_number_col'
IMAGENUMBER = 'image_number_col'
SCALING_PREFIX = 'scaling_prefix'
OBJECTNUMBER = 'object_number_col'

CON_SQLITE = 'sqlite'
CON_MYSQL = 'mysql'


"""
Default settings
"""
default_dict = {
    IMAGENUMBER: 'ImageNumber',
    OBJECTNUMBER: 'ObjectNumber',

    STACK_RELATIONS: {
        PARENT: 'Parent',
        MODNAME: 'ModificationName',
        MODPRE: 'ModificationPrefix',
        STACK: 'Stack',
        REF: 'RefStack'
    },

    STACK_DIR: {
        STACK: 'StackName',
        ID: 'index',
        NAME: 'name',
        TYPE: 'channel_type',
        SEP: ','
    },

    LAYOUT_CSV: {
    SEP: ','
    },

    BACKEND: CON_MYSQL,

    BARCODE_CSV: {
        SEP: ','
    },

    CPOUTPUT: {
        MEASUREMENT_CSV: {
            SEP: ',',
            FILETYPE: '.csv'
        },
        RELATION_CSV: {
            SEP: ',',
            OBJECTID_FROM: 'First Object Name',
            OBJECTID_TO: 'Second Object Name',
            OBJECTNUMBER_FROM: 'First Object Number',
            OBJECTNUMBER_TO: 'Second Object Number',
            IMAGENUMBER_FROM: 'First Image Number',
            IMAGENUMBER_TO: 'Second Image Number',
            RELATIONSHIP: 'Relationship'
        },
        IMAGES_CSV: {
            MASKFILENAME_PEFIX: 'ObjectsFileName_',
            SEP: ',',
            SCALING_PREFIX: 'Scaling_'
        }
    },
    PANNEL_CSV: {
        SEP: ',',
        CHANNEL_NAME: 'metal',
        DISPLAY_NAME: 'name',
        CHANNEL_TYPE_DEFAULT: 'IMC'
    }
}

"""
Required fields
"""
#TODO

"""
Functions
"""

def read_configuration(path):
    """
    Reads a configuration file into a dictionary.

    Args:
        path: path to a yaml configuration file

    Return:
        the loaded configuration file

    """

    with open(path, 'r') as stream:
        try:
            conf = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    conf = add_defaults(conf)
    return conf

def add_defaults(config):
    """
    Adds the default argument to  the configuration dictionary.

    Args:
        config: a dictionary containing the configurations

    Return:
        the updated configuration file
    """

    new_config = _update_nested_dict(default_dict, config)

    return new_config

def _update_nested_dict(input_d, u):
    """
    Updates a nested dictionary
    From: https://stackoverflow.com/a/3233356
    Example:
        >>> import pprint
        >>> a = {'a': {'b': 1, 'c': {'d': 1, 'e': 1}}, 'f': 1}
        >>> b = {'a': {'c': {'e': 3, 'g': 2}}, 'f': 2}
        >>> a_updated = _update_nested_dict(a,b)
        >>> pprint.pprint(a_updated)
        {'a': {'b': 1, 'c': {'d': 1, 'e': 3, 'g': 2}}, 'f': 2}
    """

    d = copy.deepcopy(input_d)
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = _update_nested_dict(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d
