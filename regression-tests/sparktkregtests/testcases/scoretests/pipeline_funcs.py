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

from record import Record

def string_to_numeric(row):
    result = []
    for element in row:
        result.append(int(element[1]))
    return result

def evaluate(data):
    schema = [("f1", int), ("f2", int), ("f3", int)]
    numeric_record = Record(schema, string_to_numeric(data))
    r = numeric_record.score("localhost:9114") #This is not a generic URL. It is hardcoded to match the scoring engine created for this test.
    return str(r)


