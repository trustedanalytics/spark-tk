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

from sparktk import TkContext as _TkContext  # use underscore to hide from public namespace

_cities_frame = None

def get_cities_frame(tc=_TkContext.implicit):
    """Creates a small frame of city data"""
    _TkContext.validate(tc)

    global _cities_frame
    if _cities_frame is None:

        schema = zip('rank|city|population_2013|population_2010|change|county'.split('|'),
                     [int, str, int, int, str, str])
        data = [[field for field in line.split('|')] for line in """1|Portland|609456|583776|4.40%|Multnomah
2|Salem|160614|154637|3.87%|Marion
3|Eugene|159190|156185|1.92%|Lane
4|Gresham|109397|105594|3.60%|Multnomah
5|Hillsboro|97368|91611|6.28%|Washington
6|Beaverton|93542|89803|4.16%|Washington
15|Grants Pass|35076|34533|1.57%|Josephine
16|Oregon City|34622|31859|8.67%|Clackamas
17|McMinnville|33131|32187|2.93%|Yamhill
18|Redmond|27427|26215|4.62%|Deschutes
19|Tualatin|26879|26054|4.17%|Washington
20|West Linn|25992|25109|3.52%|Clackamas
7|Bend|81236|76639|6.00%|Deschutes
8|Medford|77677|74907|3.70%|Jackson
9|Springfield|60177|59403|1.30%|Lane
10|Corvallis|55298|54462|1.54%|Benton
11|Albany|51583|50158|2.84%|Linn
12|Tigard|50444|48035|5.02%|Washington
13|Lake Oswego|37610|36619|2.71%|Clackamas
14|Keizer|37064|36478|1.61%|Marion""".split('\n')]

        _cities_frame = tc.frame.create(data, schema)

    return _cities_frame
