import pandas as pd
import os
import re
import spherpro.configuration as conf
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
    
    Example:
        >>> construct_sql_query('Table')
        'SELECT * FROM Table;'
        >>> construct_sql_query('Table', columns=['ColA', 'ColB'])
        'SELECT ColA, ColB FROM Table;'
        >>> construct_sql_query('Table', columns=['ColA', 'ColB'],\
                clauses=['B in ("c", "d")'])
        'SELECT ColA, ColB FROM Table WHERE B in ("c", "d");'

    """
    if columns is None:
        columns = ['*']
    query = ' '.join(['SELECT', ', '.join(columns),
                      'FROM', table])
    if (clauses is not None) and clauses:
            query += ' WHERE '
            query += ' AND '.join(clauses)

    query += ';'
    return query

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
    outdict = {filterdict[k]: v for k, v in indict.items()
               if ((v is not None) and (k in filterdict.keys()))}

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
