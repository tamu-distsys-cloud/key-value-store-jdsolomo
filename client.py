import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

debugging = False

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        # Create GetArgs object and pass Get call to server
        id = nrand()    # Random value to ID each server request
        args = GetArgs(key, id, 1)  # Denote that this is a pre-merge request
        try:
            primary_server_id = int(key) % self.cfg.nservers
        except ValueError:  # Handle non-numeric key values
            primary_server_id = int(ord(key)) % self.cfg.nservers
        server_index = 0
        req_success = 0
        for attempt in range(100):   # Keep trying call until success or 20 attempts reached
            server_index = primary_server_id    # Always start with primary server
            for shard_attempt in range(self.cfg.nreplicas):
                # Calculate the next server ID to try
                server_index = (primary_server_id + shard_attempt) % self.cfg.nservers
                try:
                    if debugging:
                        print("Trying to GET key={} from srvr={}".format(key, server_index))
                    reply = self.servers[server_index].call("KVServer.Get", args)
                except TimeoutError:
                    if debugging:
                        print("Retrying GET...")
                    continue
                else:
                    if debugging:
                        print("Succesful GET key={} value={} on srvr={}".format(key, reply[0], server_index))
                    req_success = 1
                    break
            else:
                if debugging:
                    print("ERROR: All shard replicas failed!")
            if (req_success == 1):
                break   # Break if request was successful to any shard
        else:
            if debugging:
                print("ERROR: GET failed!")
        
        # Only return the string (not the timestamp)
        return reply[0]

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        # Create PutAppendArgs object and pass requested call to server
        id = nrand()    # Random value to ID each server request
        args = PutAppendArgs(key, value, id)
        try:
            primary_server_id = int(key) % self.cfg.nservers
        except ValueError:  # Handle non-numeric key values
            primary_server_id = int(ord(key)) % self.cfg.nservers
        server_index = 0
        req_success = 0
        for attempt in range(100):   # Keep trying call until success or 20 attempts reached
            server_index = primary_server_id    # Always try primary server first
            for shard_attempt in range(self.cfg.nreplicas):
                # Calculate next server to try
                server_index = (primary_server_id + shard_attempt) % self.cfg.nservers
                try:
                    if debugging:
                        print("Trying to PUT/APPEND key={} value={} from srvr={}".format(key, value, server_index))
                    reply= self.servers[server_index].call("KVServer."+op, args)
                except TimeoutError:
                    if debugging:
                        print("Retrying "+op+"...")
                    continue
                else:
                    req_success = 1
                    break
            else:
                if debugging:
                    print("ERROR: "+op+" failed!")
            if (req_success == 1):  # Break if request was successful to any shard
                break
        else:
            if debugging:
                print("ERROR: "+op+" failed!")
        
        return reply

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
