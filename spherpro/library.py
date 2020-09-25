import os
import re

import networkx as nx
import pandas as pd

import spherpro.configuration as conf
import spherpro.db as db


def fill_null(data, table):
    """
    Fills a Dataframe with less columns as the DB table with None
    """
    data_cols = data.columns
    table_cols = table.__table__.columns.keys()
    uniq = list(set(table_cols) - set(data_cols))
    for un in uniq:
        data[un] = None
    return data


def find_measurementmeta(stack_name, col_name, no_stack_str=None, no_plane_string=None):
    """
    finds the measurement meta information from a given string

    Args:
        stack_name: an iterable  containing a name for all known stacks.
            exp: [DistStack, BinStackr, FullStack]
        columns: a list of column names.

    Returns:
        Returns pandas.Series, with the following strings
        in this order:
        x | measurement type | measurement name | stack name | plane id
    """
    stackpattern = "(" + "|".join(stack_name) + ")"
    pre_pattern = "^([^_]*)_(.*)"
    post_pattern = "(.*)_" + stackpattern + "_(c\d+)"
    match = re.search(pre_pattern, col_name)
    if match != None:
        match = match.groups()
        mtype = match[0]
        rside = match[1]
    else:
        return pd.Series(["", "", "", "", ""])

    match = re.search(post_pattern, rside)
    if match != None:
        match = match.groups()
        name = match[0]
        stack = match[1]
        plane = match[2]
    else:
        name = rside
        stack = no_stack_str
        plane = no_plane_string

    return pd.Series([col_name, mtype, name, stack, plane])


def construct_in_clause_list(key_dict):
    """
    Construnct a list of IN clauses.
    Args:
        key_dict: a dictionary where key is the database column key and value
            is the values to filter for.
    Return:
        a list of constructed clauses

    Example:
        >>> out = construct_in_clause_list({'A': [1], 'B': ['c','d']})
        >>> sorted(out)
        ['A IN ("1")', 'B IN ("c","d")']

    """
    querylist = [
        k + ' IN ("' + '","'.join(map(str, values)) + '")'
        for k, values in key_dict.items()
    ]
    return querylist


def filter_and_rename_dict(indict, filterdict):
    """
    Renames the  keys of the input  dict using the filterdict.
    Keys not present in the filterdict will be removed.

    Example:
        >>> import pprint
        >>> outdict =filter_and_rename_dict({'a': 1, 'b':[2,2]}, {'a': 'c', 'b': 'd', 'e': 'f'})
        >>> pprint.pprint(outdict)
        {'c': 1, 'd': [2, 2]}

    """
    outdict = {
        filterdict[k]: v
        for k, v in indict.items()
        if ((v is not None) and (k in filterdict.keys()))
    }

    return outdict


def read_csv_from_config(configdict, base_dir=None):
    """
    Read the CSV from a configuration entry.
    """
    path = configdict[conf.PATH]
    sep = configdict[conf.SEP]
    if base_dir is not None:
        path = os.path.join(base_dir, path)
    dat = pd.read_csv(path, sep=sep)
    return dat


def map_group_re(x, re_str):
    """
    Maps a regular expression with matchgroups
    to a iterable (e.g. column in a dataframe) and returns the
    result as a data frame
    Args:
        x: iterable
        re_str: a regular expression string with matchgroups
    Return:
        A dataframe with column names being matchgroups

    """
    qre = re.compile(re_str)
    m_list = [
        pd.DataFrame.from_dict([m.groupdict() for m in qre.finditer(s)]) for s in x
    ]
    return pd.concat(m_list, ignore_index=True)


def get_largest_commponent_objs(dat, keys=None):
    """
    Get the nodes of the largest connected component of a
    graph.
    Args:
        dat: a dataframe representing the edgelist
        keys: list of source and target column names
    Return:
        A list of object ids
    """
    if keys is None:
        keys = [
            db.object_relations.object_id_parent.key,
            db.object_relations.object_id_child.key,
        ]
    g = nx.from_pandas_edgelist(dat[keys], source=keys[0], target=keys[1])
    gmax = max(nx.connected_components(g), key=len)
    return pd.Series((int(n) for n in gmax), name=db.objects.object_id.key)
