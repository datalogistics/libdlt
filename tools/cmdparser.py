import argparse
import logging

def parseArgs():
    parser = argparse.ArgumentParser(
        description="Listen for and then process a particular LANDSAT scene")
    parser.add_argument('sceneInfo', help='comma seperated Scene list or regex or folder')
    parser.add_argument('-s', '--scenes', action='store_true', help='Comma-separated list of scenes to look for',default=True)
    parser.add_argument('-f', '--folder', action='store_true', help='Folder name to download')
    parser.add_argument('-r', '--regex', action='store_true', help='Comma-separated list of scenes to look for')
    parser.add_argument('-F', '--filter', type=str, help='filter files by unix wildcard')
    parser.add_argument('-H', '--host', type=str, help='The Exnode service',
                        default="http://dev.crest.iu.edu:8888")
    parser.add_argument('-X', '--visualize', type=str, help='Enable visualization')
    parser.add_argument('-S', '--ssl', action='store_true', help='Use ssl for socket connection')
    parser.add_argument('-v', '--verbose', action='store_true', help='Produce verbose output from the script')
    parser.add_argument('-p', '--path',action='store_true', help='path to folder to download')
    args = parser.parse_args()
    # Set log stuff
    form  = '[%(asctime)s] %(levelname)7s:%(msg)s'
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format = form, level = level)

    return args
