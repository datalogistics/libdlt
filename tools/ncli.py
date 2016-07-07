#!/usr/bin/env python

""" A Cli app to browse UNIS , exnodes and Blipp """
import os
import sys
import cmd
import readline
from subprocess import call
import signal
import json
import logging
import requests
from subprocess import Popen
import urllib
from functools import partial
from urlparse import urlparse

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
opmap = {'!=': 'not=eq=',
         '<': 'lt',
         '<=': 'lte',
         '=': 'eq',
         '>': 'gt',
         '>=': 'gte',
         'and': 'and',
         'or': 'or',
         'reg': 'reg'}
def signal_handler(signal, frame):
    print('Exiting the program')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def completer(text, state):
    options = [x for x in addrs if x.startswith(text)]
    try:
        return options[state]
    except IndexError:
        return None

readline.set_completer(completer)
readline.parse_and_bind("tab: complete")
## Utils
def display(s) :
    print str(s)

def display_error(str) :
    print str

### Request utils
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

def get (url, cb , cert=None,errorCb = None) :
    """ Get json from url and return the processed data through cb or errorCb """
    if not cert or not cert['ssl']:
        r = (requests.get(url)).json()
    else :
        r = (requests.get(url,cert=(cert['cert'],cert['key']))).json()
    return r

def get_query_params (arr) :
    """ consume as many as required and recursively solve it """
    s = ""
    showHelp = False
    global opmap
    print arr
    while len(arr) >= 2 :
        if arr[0] == '?' :
            showHelp = True
        elif arr[1] in opmap :
            s+= arr[0] + "=" + opmap[arr[1]] + "=" + arr[2] + "&"
        arr = arr[3:]
    return s,showHelp

#### Globals - All prefixed with global
global_last_json = []
global_cmd_arr = []
global_available_cd = {}
global_cdp = [0,0,0]    ## Stores a parent query list

def get_unis_list() :
    obj = {}
    for i in unislist:
        obj[i] = {'help' : "Url " + get_url(unislist[i]) }
    return obj

def processUnisTop (json) :
    """ Process unis top level and get list of commands """
    cmd = {}
    for i in json :
        o = urlparse(i['href'])
        name  = o.path[1:]  # remove /
        cmd[name] = {'help' : "Full Url is " + i['href']}
    return cmd

def init_availabe_commands() :
    """ Use global cmd arr to populate global_available_cd """
    global global_available_cd
    global global_cmd_arr
    length  = len(global_cmd_arr)
    if length == 0:
        global_available_cd = {
            'unis' : { 'help' : "Show unis servers" },
            'blipp': { 'help' : "INCOMPLETE - Show blipp instances"},
            'harvester': { 'help' : "INCOMPLETE - Show harvester instances"}
            }
    elif global_cmd_arr[0] == 'unis' :
        if length == 1 :
            global_available_cd = get_unis_list()
        elif length == 2 :
            ob = unislist[global_cmd_arr[1]]
            u = get_url(ob)
            try :
                j = get(get_url(ob),ob)
            except Exception as e:
                print e
            else :
                global_available_cd = processUnisTop(j)
        elif length > 2 :
            obj = {}
            obj['Keys to filter by'] = { 'help' : "kk, kk  , lll"}
            for i in opmap :
                obj[i] = { 'help' : "Query stuff " }
            global_available_cd = obj
    else :
        global_available_cd = { '?' : 'Unimplemented' }

def show_help(a=None,b=None) :
    display("Help : ")
    ls = command_map.keys()
    display("Possilbe commands are : " + ",".join(ls))
    for i in command_map :
        display(i + " -> " + command_map[i]['help'])

def show_last_json(arr) :
    global global_last_json
    if len(arr) <= 1 :
        for i in range(0,len(global_last_json)) :
            j = len(global_last_json) - i-1
            display(str(i) + " -> " + global_last_json[i]['info'])
    else :
        num = -1
        try :
            num = int(arr[1])
        except :
            display("Please enter a number ")
        else :
            if num >= 0 and num <= 5 :
                display(arr[1] + " -> " + global_last_json[num]['info'])
            else :
                display("Enter number between 0 and 5 ")

def show (arr):
    """ Parse the last cmd array , add to currently given array - get url and headers
    request , display and store in last_json """
    global global_last_json
    global global_cmd_arr
    global global_cdp
    ar = []
    ar.extend(global_cmd_arr)
    ar.extend(arr[1:])
    if ar[0] == 'unis' :
        if ar[1] in unislist :
            ob = unislist[ar[1]]
            q,h = get_query_params(ar[3:])
            qp,h = get_query_params(global_cdp)
            if q :
                q = q + "&" + qp
            else :
                q = qp
            if len(ar) > 2 :
                u = get_url(ob) + "/"+ar[2] + "?" + q
            else :
                u = get_url(ob)
            display("Url is " + u )
            try :
                j = get(u,ob)
            except Exception as e :
                print e
            else :
                ob = global_last_json or []
                mp = {}
                mp['info'] = " ".join(ar)
                mp['data'] = j
                global_last_json = [mp]
                global_last_json.extend(ob[1:5])
                display(j)
        else :
            display("Not Present in unisList")
    else :
        display("Not implemented ")

# defining all the commands defined in the command map
def cd_cmd(arr) :
    """ Basically pushes the last used command to `global_cmd_arr` and remove it when .. is used , clears it if arr is empty """
    ar = arr[1:]
    global global_cmd_arr
    global global_available_cd
    err = False
    if len(ar) == 0:
        global_cmd_arr = []
    elif ar[0] == ".." :
        global_cmd_arr.pop() if len(global_cmd_arr) > 0 else None
    elif ar[0] in global_available_cd :
        global_cmd_arr.append(ar[0])
    else :
        err = True
    init_availabe_commands()
    if err :
        display("Invalid arguments , available arguments are")
        show_help()
    elif len(ar) <= 1 :
        display("Current stack is " + " ".join(global_cmd_arr))
    else :
        cd_cmd(ar)

def cdp_cmd (arr) :
    """ Gets a list of ids and sets as parent filter for the exnodes """
    global global_last_json
    global global_cmd_arr
    global global_cdp
    if len(arr) == 1 :
        global_cdp = []
        display("Removed any parent filter ")
        return
    ar = []
    ar.extend(global_cmd_arr)
    ar.extend(arr[1:])
    if len(ar) < 3 :
        display("Need to cd more to set parent query , the current status is ")
        pwd_cmd()
        return

    if ar[0] == 'unis' :
        if ar[1] in unislist :
            ob = unislist[ar[1]]
            q,h = get_query_params(ar[3:])
            if len(ar) > 2 :
                u = get_url(ob) + "/"+ar[2] + "?" + q
            else :
                u = get_url(ob)
            display("Url is " + u )
            try :
                j = get(u,ob)
            except Exception as e :
                print e
            else :
                try :
                    parr = map((lambda x : x['id']),j)
                except Exception as e:
                    print e
                else :
                    global_cdp[0] = 'parent'
                    global_cdp[1] = '='
                    ### number of ids are limited to 10
                    global_cdp[2] = ",".join(parr[:10])
        else :
            display("Not Present in unisList")
    else :
        display("Not implemented ")




def pwd_cmd(arr=None) :
    display(global_cmd_arr)

def ls_cmd(arr):
    """ Display all available commands with help """
    for i in global_available_cd :
        display(i + " -> " + global_available_cd[i]['help'])

def export_cmd(arr):
    """ Export given json or last json to file """
    global global_last_json
    name = os.path.expanduser('~') + "/temp.json"
    i = 0
    if len(arr) == 1 :
        """ Export last to default file in home """
        i = 0
    elif len(arr) > 1 :
        try :
            i = int(arr[1])
        except:
            # Use arr[1] as filename
            name = arr[1]
        else:
            name = arr[1] if len(arr) >= 2 else name

    if i < len(global_last_json) :
        f = open(name,"w")
        f.write(str(global_last_json[i]['data']))
        f.close()
        display("File written to " + os.path.abspath(f.name))
    else :
        display("No json fetched available at index "+ str(i))

command_map = {
    'ls' : {
        'f' : ls_cmd,
        'help' : "Lists out stuff "
        },
    'cd' : {
        'f' : cd_cmd ,
        'help' : ""
        },
    'cdp' : {
        'f' : cdp_cmd,
        'help' : "Specially for exnodes - sets the query as parent "
        },
    'show' : {
        'f' : show,
        'help' : "print json"
        },
    'export': {
        'f' : export_cmd,
        'help' : "Export json to file - export <index> json"
        },
    'last' :{
        'f' : show_last_json,
        'help' : "Shows a command list of last printed json - stores upto 5 json"
        },
    'help' : {
        'f' : show_help,
        'help' : "Shows Help"
        },
    'pwd' : {
        'f' : pwd_cmd,
        'help' : "Show pwd"
        },
    '?' : {
        'f' : show_help,
        'help' : "Shows Help"
        },
    'exit' : {
        'f' : (lambda x=None,y=None : sys.exit(0)),
        'help' : "exit"
        }
    }


"""Parsing infrastructure based on command map - basically redirects to different command based on first command """
def parse_args(s) :
    """ Check from commands map and use the args to run the command """
    tokens = s.split(" ")
    """ lower case everything and filter out empty """
    tokens = map((lambda x : x.strip().lower()), tokens)
    arr = filter((lambda x : True if x else False), tokens)
    runCommand(arr)

def runCommand(arr):
    """ Looksup in command map and runs the required commands """
    if not arr :
        return
    if arr[0] in command_map :
        command_map[arr[0]]['f'](arr)
    else :
        show_help()

def main() :
    init_availabe_commands() ### Initialize availabe commands
    while 1:
        parse_args(raw_input("> "))

if __name__ =="__main__" :
    main()
