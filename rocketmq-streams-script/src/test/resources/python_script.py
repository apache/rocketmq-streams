#!/usr/bin/env python
# coding=utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json;
import re;

regex = '^/(.*)/\w+'
pattern = re.compile(regex)

def pythonTest(*processLine):
    try:
        jsonObject = json.loads(processLine[0])

        if jsonObject.has_key('filepath'):
            filePath = jsonObject['filepath']
            match = pattern.search(filePath)
            if match:
                return match.group(1)
        else:
            pass # print "does not has key filepath"
    except BaseException as e:
        pass # print "process one line cause exception %s" %e
    return "does not match"
