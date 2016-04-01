#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Utility designed to print pretty things to the
standard output. It consists of a dictionary
that is called as a function with a key. A
string is then returned that helps format
terminal output.

'''
from termcolor import colored as color


def item(key='bullet'):
    '''
    Looks for a formatting string in a dictionary.

    Parameters:
    ----------
    A string among the ones available in the function:

      - bullet
      - warn
      - error
      - success

    Returns:
    -------
    A byte object.

    '''
    dictionary = {
        'bullet': color(' →', 'blue', attrs=['bold']),
        'warn': color(' WARN:', 'yellow', attrs=['bold']),
        'error': color(' ERROR:', 'red', attrs=['bold']),
        'success': color(' SUCCESS:', 'green', attrs=['bold'])
    }

    return dictionary[key]
