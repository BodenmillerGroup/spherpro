import pandas as pd
import re

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
        return pd.Series([None, None, None, None, None])

    match = re.search(post_pattern, rside)
    if match != None:
        match = match.groups()
        name = match[0]
        stack = match[1]
        plane = match[2]
    else:
        name = rside
        stack = no_stack_str
        plane = None

    return pd.Series([x, mtype, name, stack, plane])
