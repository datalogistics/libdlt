""" A Cli app to browse UNIS , exnodes and Blipp """
# import os
import sys
import cmd
import readline
from subprocess import call
import signal
import json
import logging
import requests
from subprocess import Popen
import cmdparser
import urllib
from functools import partial
from urlparse import urlparse
## Re
def signal_handler(signal, frame):
    print('Exiting the program')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

### lets print arguments
def main()  :
    xString = raw_input("Enter a filename")
    try:
        print open(xString).read()
    except:
        print "File doesn't exist"

def completer(text, state):
    options = [x for x in addrs if x.startswith(text)]
    try:
        return options[state]
    except IndexError:
        return None

readline.set_completer(completer)
readline.parse_and_bind("tab: complete")
map = {}
def display(s) :
    print str(s)

def display_error(str) :
    print str

def showCommands() :
    ls = commands.keys()
    display("Commands are ")
    for i in ls:
        display(i)

def get_url(obj):
    url = ""
    if obj.get('ssl') :
        url += "https://"
    else :
        url += "http://"
    url += obj['host']
    if 'port' in obj :
        url +=":" + str(obj['port'])
    return url

def showUnisList(arr,index) :
    display("List of Unis")
    for i in unislist:
        display(i + "-> " + get_url(unislist[i]))

def get (url, cb , cert=None,errorCb = None) :
    """ Get json from url and return the processed data through cb or errorCb """
    try:
        if not cert or not cert['ssl']:
            r = (requests.get(url)).json()
        else :
            r = (requests.get(url,cert=(cert['cert'],cert['key']))).json()
        return cb(r)
    except Exception as e:
        print "Error geting url ", url , "\n"
        if errorCb :
            return errorCb(url)
        else :
            print e

def processUnisTop (json) :
    """ Process unis top level and get list of commands """
    cmd = {}
    for i in json :
        o = urlparse(i['href'])
        name  = o.path[1:]  # remove /
        cmd[name] = { 'f' : (lambda x,y : get(i['href'],display))}
    return cmd

def getQueryParams (arr) :
    """ consume as many as required and recursively solve it """
    s = ""
    # Index based
    complist = ['>','<','=','<=','>=','!=','reg','and','or']
    opmap = {'!=': 'not=eq=',
         '<': 'lt',
         '<=': 'lte',
         '=': 'eq',
         '>': 'gt',
         '>=': 'gte',
         'and': 'and',
         'or': 'or',
         'reg': 'reg'}
    showHelp = False
    while len(arr) > 1 :
        if arr[0] == '?' :
            showHelp = True
        elif arr[0] in opmap :
            s += opmap[arr[0]] + "=" + arr[1] + "&"
        arr = arr[2:]
    return s , showHelp

def getUnisListCommands(unisob,arr,ind) :
    query, showHelp = getQueryParams(arr[ind:])
    return [], 1 , get(get_url(unisob)+"?"+query,processUnisTop,unisob)

def getUnisList(arr,index) :
    cmd = {}
    for i in unislist:
        cmd[i] = { 'getCommands' : partial(getUnisListCommands,unislist[i]) }
    return arr , index , cmd

# Can contain either f , getCommands or <command names> (recurrsively) . f terminates sequence
commands = {
    "list" : {
        'unis' : { 'f' : showUnisList },
        'blipp' : {},
        'harvester' : {}
    },
    'cd' : {
        'getCommands' : getUnisList,
    },
    "show" : { },
    "exit" : { 'f' : (lambda x,y : sys.exit(0)) }
}
unislist = {
    "dev" : {
        "host" : "dev.crest.iu.edu",
        "port" : 8888,
        "ssl"  : False
    },
    "dlt" : {
        "host" : "dlt.crest.iu.edu",
        "port" : 9000,
        "ssl"  : True,
        "cert"  : "./dlt-client.pem",
        "key"  : "./dlt-client.pem"
    },
    "monitor" : {
        "host" : "monitor.crest.iu.edu",
        "port" : 9000,
        "ssl" : False
    }
}

def parseArgs(str) :
    """ Check from commands map and use the args to run the command """
    tokens = str.split(" ")
    parseCommands(tokens,0,  commands )

def showHelp(arr,index,cmd) :
    ls = cmd.keys()
    ls.pop('f') if 'f' in cmd else None
    ls.pop('getCommands') if 'getCommands' in cmd else None
    display("Commands are ")
    for i in ls:
        display(i)

def parseCommands (arr,index,cmd) :
    """ arr is array and index denotes position of command
    cmd contains either a function f or function getcommand or list of future commands as part of map """
    if not cmd :
        """ No cmd object means parsing is done """
        display("Nothing to do - Probably incomplete functionality")
        return
    elif 'f' in cmd :
        """ f terminates the sequence """
        cmd['f'](arr,index)
    elif 'getCommands' in cmd :
        narr,nind,ncmd, = cmd['getCommands'](arr,index)
        parseCommands(narr,nind,ncmd) if ncmd else None
    elif index >= len(arr):
        """ Maybe just end of stuff to do - Show as nothing to do """
        # display("Nothing to do - Probably incomplete functionality")
        showHelp(arr,index,cmd)
    elif arr[index] in cmd :
        parseCommands(arr,index + 1,cmd[arr[index]])
    else :
        """ Display the help message for the remaining cmd """
        showHelp(arr,index,cmd)

if __name__ =="__main__" :
    while 1:
        parseArgs(raw_input("> "))
