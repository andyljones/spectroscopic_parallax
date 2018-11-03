import json

def config(key):
    return json.load(open('config.json'))[key]