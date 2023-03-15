from __future__ import absolute_import

""" __main__.py - Parse, Query, Build, and Modify IOS-style configurations
     Copyright (C) 2023      David Michael Pennington at Starbucks Coffee Co
"""

# Follow PEP366...
# https://stackoverflow.com/a/6655098/667301
if (__name__ == "__main__") and (__package__ is None):
    import sys
    import os

    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(1, parent_dir)
    import polymer
    __package__ = str("polymer")
    del sys, os
