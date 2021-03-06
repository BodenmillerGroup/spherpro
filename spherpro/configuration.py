import collections
import copy

import yaml

import spherpro.db as db

"""
This module defines the configuration file structure,
defaults, required files, loading as well as validation functions.
"""

"""
List of keywords fields used in the configuration file
"""
QUERY_DEFAULTS = "query_defaults"
CHANNEL_MEASUREMENTS = "channel_measurements"
RAWDIST = "distance_measure_raw"
CORRDIST = "distance_measure_corrected"
OBJECT_DEFAULTS = "object_defaults"
DEFAULT_OBJECT_TYPE = db.objects.object_type.key
DEFAULT_MEASUREMENT_NAME = db.measurement_names.measurement_name.key
DEFAULT_STACK_NAME = db.stacks.stack_name.key
DEFAULT_MEASUREMENT_TYPE = db.measurement_types.measurement_type.key
DEFAULT_OBJ_TYPE = db.objects.object_type.key
DEFAULT_CHANNEL_NAME = db.ref_planes.channel_name.key
DEFAULT_CHANNEL_TYPE = db.ref_planes.channel_type.key

BACKEND = "backend"
BARCODE_CSV = "barcode_csv"
CHANNEL_NAME = "channel_name_col"
CHANNEL_TYPE = "channel_type"
CHANNEL_TYPE_DEFAULT = "IMC"
CONDITION = "condition_col"
CPOUTPUT = "cpoutput"
CP_DIR = "cp_dir"
DEFAULT_OBJECT = "default_object"
DISPLAY_NAME = "display_name_col"
FILENAME_COL = "filename_col"
FILETYPE = "filetype"
GROUP_SITE = "group_site"
ID = "id_col"
IMAGENUMBER = "image_number_col"
IMAGENUMBER_FROM = "first_image_number_col"
IMAGENUMBER_TO = "second_image_number_col"
IMAGES_CSV = "images_csv"
LAYOUT_CSV = "layout_csv"
MASK_FILENAME_PREFIX = "mask_filename_col_prefix"
STACKIMG_FILENAME_PREFIX = "stackimg_filename_col_prefix"
MEASUREMENT_CSV = "measurement_csv"
MODNAME = "modname_col"
MODPRE = "modpre_col"
NAME = "name_col"
OBJECTTYPE = "object_type"
OBJECTTYPE_FROM = "first_object_type_col"
OBJECTTYPE_TO = "second_object_type_col"
OBJECTNUMBER = "object_number_col"
OBJECTNUMBER_FROM = "first_object_number_col"
OBJECTNUMBER_TO = "second_object_number_col"
OBJECTS = "objects"
PANEL_CSV = "pannel_csv"
PANNEL_CSV = "pannel_csv"
PANNEL_ID = "pannel_id"
PARENT = "parent_col"
PATH = "path"
PLATE = "plate_col"
REF = "ref_col"
RELATIONSHIP = "relationship"
RELATION_CSV = "relation_csv"
SCALING_PREFIX = "scaling_prefix"
SEP = "sep"
STACK = "stack_col"
STACK_DIR = "stack_dir"
STACK_RELATIONS = "stack_relations"
TYPE = "type_col"
WELL_COL = "well_col"
MASK_DIR = "mask_dir"
STACKIMG_DIR = "stackimg_dir"
GROUP_SITE = "group_site"
GROUP_CROPID = "group_cropid"
GROUP_POSX = "group_x"
GROUP_POSY = "group_y"
GROUP_SLIDEAC = "group_slideac"
GROUP_PANORMAID = "group_panoid"
GROUP_ACID = "group_acid"
GROUP_ROIID = "group_roiid"
GROUP_SLIDENUMBER = "group_slidenumber"
GROUP_SAMPLEBLOCKNAME = "group_sampleblockname"
GROUP_BASENAME = "group_basename"

META_REGEXP = "re_meta"
IMAGE_OME_FOLDER_DIRS = "ome_folder_dirs"
IMAGE_OME_META_REGEXP = "ome_meta_regexp"
IMAGE_SLIDE_REGEXP = "slide_regexp"
IMAGE_HEIGHT_PREFIX = "image_height_col_prefix"
IMAGE_WIDTH_PREFIX = "image_width_col_prefix"

CON_SQLITE = "sqlite"
CON_MYSQL = "mysql"
CON_POSTGRESQL = "postgresql"

LAYOUT_CSV_PLATE_NAME = "plate_col"
LAYOUT_CSV_WELL_NAME = "well_col"
LAYOUT_CSV_COND_NAME = "condition_col"
LAYOUT_CSV_CONTROL_NAME = "control_col"
LAYOUT_CSV_TIMEPOINT_NAME = "timepoint_col"
LAYOUT_CSV_BC_PLATE_NAME = "bc_plate_col"
LAYOUT_CSV_CONCENTRATION_NAME = "concentration_col"
LAYOUT_CSV_BARCODE = "barcode_col"
LAYOUT_CSV_COND_ID = "condition_id_col"

BC_CSV_PLATE_NAME = "plate_col"
BC_CSV_WELL_NAME = "well_col"

PANEL_CSV_CHANNEL_NAME = "channel_name"
PANEL_CSV_DISPLAY_NAME = "display_name"
PANEL_CSV_ILASTIK_NAME = "ilastik_name"
PANEL_CSV_BARCODE_NAME = "barcode_name"
PANEL_CSV_CLONE_NAME = "clone_name"
PANEL_CSV_CONCENTRATION_NAME = "concentration_name"
PANEL_CSV_TUBE_NAME = "tube_name"
PANEL_CSV_TARGET_NAME = "target"

COLMAP = "column_map"

"""
Default settings
"""
default_dict = {
    QUERY_DEFAULTS: {
        DEFAULT_OBJECT_TYPE: "cell",
        CHANNEL_MEASUREMENTS: {
            DEFAULT_MEASUREMENT_NAME: "MeanIntensityComp",
            DEFAULT_STACK_NAME: "FullStackFiltered",
            DEFAULT_MEASUREMENT_TYPE: "Intensity",
        },
        RAWDIST: {
            DEFAULT_MEASUREMENT_NAME: "MeanIntensity",
            DEFAULT_STACK_NAME: "DistStack",
            DEFAULT_MEASUREMENT_TYPE: "Intensity",
            DEFAULT_CHANNEL_NAME: "dist-sphere",
        },
        CORRDIST: {
            DEFAULT_STACK_NAME: "ObjectStack",
            DEFAULT_MEASUREMENT_TYPE: "Location",
            DEFAULT_MEASUREMENT_NAME: "dist-rim",
            DEFAULT_CHANNEL_NAME: "object",
            DEFAULT_CHANNEL_TYPE: "object",
        },
        OBJECT_DEFAULTS: {
            DEFAULT_STACK_NAME: "ObjectStack",
            DEFAULT_CHANNEL_NAME: "object",
            DEFAULT_CHANNEL_TYPE: "object",
        },
    },
    IMAGENUMBER: "ImageNumber",
    OBJECTNUMBER: "ObjectNumber",
    OBJECTTYPE: "ObjectID",
    STACK_RELATIONS: {
        PARENT: "Parent",
        MODNAME: "ModificationName",
        MODPRE: "ModificationPrefix",
        STACK: "Stack",
        REF: "RefStack",
    },
    STACK_DIR: {
        STACK: "StackName",
        ID: "index",
        NAME: "name",
        TYPE: "channel_type",
        SEP: ",",
    },
    LAYOUT_CSV: {
        PATH: None,
        LAYOUT_CSV_PLATE_NAME: "plate",
        LAYOUT_CSV_BC_PLATE_NAME: "bc_plate",
        LAYOUT_CSV_WELL_NAME: "TargetWell",
        LAYOUT_CSV_COND_NAME: None,
        LAYOUT_CSV_CONTROL_NAME: "control",
        LAYOUT_CSV_TIMEPOINT_NAME: None,
        LAYOUT_CSV_CONCENTRATION_NAME: "concentration",
        LAYOUT_CSV_BARCODE: "barcode",
        LAYOUT_CSV_COND_ID: "conditionID",
        SEP: ",",
    },
    BACKEND: CON_MYSQL,
    BARCODE_CSV: {
        PATH: None,
        BC_CSV_PLATE_NAME: "Plate",
        BC_CSV_WELL_NAME: "Well",
        SEP: ",",
    },
    CPOUTPUT: {
        MEASUREMENT_CSV: {SEP: ",", FILETYPE: ".csv", DEFAULT_OBJECT: "cell"},
        RELATION_CSV: {
            SEP: ",",
            OBJECTTYPE_FROM: "First Object Name",
            OBJECTTYPE_TO: "Second Object Name",
            OBJECTNUMBER_FROM: "First Object Number",
            OBJECTNUMBER_TO: "Second Object Number",
            IMAGENUMBER_FROM: "First Image Number",
            IMAGENUMBER_TO: "Second Image Number",
            RELATIONSHIP: "Relationship",
        },
        IMAGES_CSV: {
            MASK_FILENAME_PREFIX: "ObjectsFileName_",
            STACKIMG_FILENAME_PREFIX: "FileName_",
            SEP: ",",
            SCALING_PREFIX: "Scaling_",
            # in CP width and height seem to be switched
            IMAGE_HEIGHT_PREFIX: "Height_",
            IMAGE_WIDTH_PREFIX: "Width_",
            META_REGEXP: (
                "(?P<{}>.*)_l(?P<{}>[0-9]*)_x(?P<{}>[0-9]*)_y(?P<{}>[0-9]*).tiff".format(
                    "basename",
                    db.images.crop_number.key,
                    db.images.image_pos_x.key,
                    db.images.image_pos_y.key,
                )
            ),
            MASK_DIR: None,  # default take cpoutput dir
            GROUP_BASENAME: "basename",
            GROUP_CROPID: db.images.crop_number.key,
            GROUP_SITE: db.sites.site_name.key,
            GROUP_POSX: db.images.image_pos_x.key,
            GROUP_POSY: db.images.image_pos_y.key,
            IMAGE_OME_FOLDER_DIRS: [],
            IMAGE_OME_META_REGEXP: (
                "(?P<{}>.*)_s0_p(?P<{}>[0-9]+)_r(?P<{}>[0-9]+)_a(?P<{}>[0-9]+)_ac.*".format(
                    db.slideacs.slideac_name.key,
                    db.sites.site_mcd_panoramaid.key,
                    db.acquisitions.acquisition_mcd_roiid.key,
                    db.acquisitions.acquisition_mcd_acid.key,
                )
            ),
            GROUP_SLIDEAC: db.slideacs.slideac_name.key,
            GROUP_PANORMAID: db.sites.site_mcd_panoramaid.key,
            GROUP_ACID: db.acquisitions.acquisition_mcd_acid.key,
            GROUP_ROIID: db.acquisitions.acquisition_mcd_roiid.key,
            # IMAGE_SLIDE_REGEXP: '.*_(?P<{}>.*)_slide(?P<{}>[0-9]+)_'.format(
            #    db.sampleblocks.sampleblock_name.key, db.slides.slide_number.key),
            IMAGE_SLIDE_REGEXP: ".*_slide(?P<{}>[0-9]+)_".format(
                db.slides.slide_number.key
            ),
            GROUP_SLIDENUMBER: db.slides.slide_number.key,
            GROUP_SAMPLEBLOCKNAME: db.sampleblocks.sampleblock_name.key,
        },
    },
    PANNEL_CSV: {
        SEP: ",",
        PANEL_CSV_CHANNEL_NAME: "metal",
        PANEL_CSV_DISPLAY_NAME: "name",
        PANEL_CSV_ILASTIK_NAME: "ilastik",
        PANEL_CSV_BARCODE_NAME: "barcode",
        PANEL_CSV_CLONE_NAME: "Antibody Clone",
        PANEL_CSV_CONCENTRATION_NAME: "Final Concentration / Dilution",
        PANEL_CSV_TUBE_NAME: "Tube Number",
        PANEL_CSV_TARGET_NAME: "Target",
    },
}

"""
Required fields
"""
# TODO

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

    with open(path, "r") as stream:
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
