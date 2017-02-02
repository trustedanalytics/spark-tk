# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# _selection is meant to be a private place to implement functionality for
# auto- model and parameter selection.  The functions will be imported to
# the desired locations and namespaces rather than referenced here.

# These import statements and __all__ declaration are ONLY intended for use
# by pdoc which will generate the API python documentation.  This solution
# is one approach, there may well be a more appropriate method.  Since this
# file (module) is considered private, it is deemed OK for such tomfoolery.

from cross_validate import cross_validate
from grid_search import grid_search

__all__ = ['cross_validate', 'grid_search']
