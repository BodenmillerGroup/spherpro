import pandas as pd
import re

def calculate_real_dist_rim(dist, radius_cut, radius_sphere):
    """calculate_real_dist_rim
    Calculates the real distance to rim from the real sphere radius, the radius
    of the cut and the measured distance to rim.

    Args:
        dist: Float stating the measured distance to rim
        radius_cut: Float stating the radius of the segment
        radius_sphere: float stating the real radius of the sphere

    Returns:
        Float real distance to rim
    """
    real_dist = radius_sphere - np.sqrt(radius_sphere**2 - 2*radius_cut*dist + dist**2)
    return real_dist


def find_measurementmeta(stackpattern, x,
                  no_stack_str = "NoStack"):
    """
    finds the measurement meta information from a given string

    Args:
        stackpattern: a string containing a capture group for all known stacks.
            exp: '(DistStack|BinStack|FullStack)'
        x: the variable string returned by Cellprofiler.

    Returns:
        Returns pandas.Series, with the following strings
        in this order:
        x | measurement type | measurement name | stack name | plane id
    """
    pre_pattern = '^([^_]*)_(.*)'
    post_pattern = '(.*)_'+stackpattern+'_(c\d+)'
    match = re.search(pre_pattern,x)
    if match != None:
        match = match.groups()
        mtype = match[0]
        rside = match[1]
    else:
        return pd.Series(['', '', '', '', ''])

    match = re.search(post_pattern, rside)
    if match != None:
        match = match.groups()
        name = match[0]
        stack = match[1]
        plane = match[2]
    else:
        name = rside
        stack = no_stack_str
        plane = ''

    return pd.Series([x, mtype, name, stack, plane])

def construct_in_clause_list(key_dict):
    querylist = [k + ' IN ("'+ '","'.join(
        map(str, values)) + '")'
                 for k, values in key_dict.items()]
    return querylist

def construct_sql_query(table, columns=None, clauses=None):
    """
    Constructs an sql query with possibilty for selection clauses

    Args:
        table: the table name
        columns: the selected columns, default: all
        clauses: a dict with columns to potentially use for filtering
                default: no filter
    Return:
        the constructed query
    """
    if columns is None:
        columns = ['*']
    query = ' '.join(['SELECT', ', '.join(columns),
                      'FROM', table])
    if len(clauses) > 0:
            query += ' WHERE '
            query += ' AND '.join(clauses)

    query += ';'
    return query

def filter_and_rename_dict(indict, filterdict):
    """
    Renames the  keys of the input  dict using the filterdict.
    Keys not present in the filterdict will be removed.
    """
    outdict = {filterdict[k]: v for k, v in indict.items()
               if ((v is not None) and (k in filterdict.keys()))}

    return outdict
    
    
