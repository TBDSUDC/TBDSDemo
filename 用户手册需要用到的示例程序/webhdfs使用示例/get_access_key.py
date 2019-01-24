#!/usr/bin/env python

import hmac
import time
import random
import base64
import urllib
import requests
import os
import json

from hashlib import sha1


def get_config(pth=None):
    if not pth:
        pth = os.path.join(os.getcwd(), 'tbds.api.json')
    if not os.path.exists(pth):
        raise OSError("Config file doesn't exist!")
    else:
        with open(pth, 'r') as fp:
            return json.load(fp)


def get_timestamp():
    return int(time.time() * 1000)


def get_access_key(sec_key, sec_id):
    nonce = str(random.randint(1, 100))
    ts = str(get_timestamp())
    raw = ''.join([sec_id, ts, nonce])
    hashed = hmac.new(sec_key, raw, sha1).digest()
    signature = urllib.quote_plus(base64.b64encode(hashed).encode('utf-8'))
    access_key = "{0} {1} {2} {3}".format(sec_id, ts, nonce, signature)
    return access_key

def main():

    config = get_config()
    #portal_ip = config['portal']['ip']
    #user_id = config['user']['id']
    sec_id = config['user']['secure_id']
    sec_key = config['user']['secure_key']
    access_key = get_access_key(str(sec_key), str(sec_id))
    print json.dumps({'Signature': access_key}, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
