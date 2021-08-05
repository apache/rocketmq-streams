#!/usr/bin/env python
# coding=utf-8
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
