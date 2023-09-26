# MIT License
#
# Copyright (c) 2022 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import copy

def merge_dictionaries_recursive(d1, d2):
    """
    Perform a deep recursive merge of two dictionaries.  The first dictionary (d1) takes
    precedence in the event of duplicate keys

    :param d1: first dictionary to merge
    :param d2: second dictionary to merge
    :return: merged dictionary
    """
    if d1 is None:
        return copy.deepcopy(d2)
    if d2 is None:
        return copy.deepcopy(d1)
    if not isinstance(d1,dict) or not isinstance(d2,dict):
        return copy.deepcopy(d1)
    merged = {}
    for key in d1:
        if key not in d2:
            merged[key] = copy.deepcopy(d1[key])
        else:
            merged[key] = merge_dictionaries_recursive(d1[key],d2[key])
    for key in d2:
        if key not in d1:
            merged[key] = copy.deepcopy(d2[key])
    return merged