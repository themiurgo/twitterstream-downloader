# coding: utf-8
from __future__ import unicode_literals

import hashlib
import json
import tempfile
import random

class Keychain(object):
    """A set of user key/secrets authenticated to a consumer key/secret.

    A keychain is identified by a unique consumer key/secret and contains one or
    more user key/secrets.

    """
    def __init__(self):
        self.__state = {}

    @property
    def state(self):
        return self.__state

    def __cmp__(self, other):
        return self.__state.__cmp__(other.__state)

    def set_consumer(self, key, secret):
        self.__state['__consumer__'] = {"key": key, "secret": secret}

    def get_consumer(self):
        c = self.__state['__consumer__']
        return [c['key'], c['secret']]

    def set_user(self, key, secret, label=None):
        assert '__consumer__' in self.__state
        if not label:
            label = key
        self.__state[label] = [key, secret]

    def get_user(self, label=None):
        assert '__consumer__' in self.__state
        if not label:
            labels = self.__state.keys()
            labels.remove("__consumer__")
            label = random.choice(labels)
        return self.__state[label]

    def rename(self, old_label, new_label):
        s = self.__state
        s[new_label] = s.pop(old_label)

    def save(self, fname):
        with open(fname, "w+") as f:
            f.write(json.dumps(self.__state, indent=2))

    def load(self, fname):
        with open(fname, "r") as f:
            self.__state = json.loads(f.read())

def test():
    k = Keychain()
    k.set_consumer("ck", "cs")
    k.get_consumer()
    k.set_user("uk1", "us1", "user1")
    k.set_user("uk2", "us2", "user2")
    k.set_user("uk3", "us3")
    k.get_user("user1")
    k.get_user("uk3")
    tf = tempfile.NamedTemporaryFile()
    k.save(tf.name)
    k2 = Keychain()
    k2.load(tf.name)
    assert k == k2

if __name__ == "__main__":
    test()
