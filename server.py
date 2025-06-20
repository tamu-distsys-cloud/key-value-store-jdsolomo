import logging
import threading
from typing import Tuple, Any
from collections import deque
import time

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key: str, value: str, id):
        self.key = key
        self.value = value
        self.id = id

    def GetKey(self):
        return self.key
    
    def GetValue(self):
        return self.value
    
    def GetID(self):
        return self.id
    
    def SetKeyValue(self, key: str, value: str):
        self.key = key
        self.value = value

    def SetID(self, id):
        self.id = id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

    def GetValue(self):
        return self.value
    
    def SetValue(self, value: str):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key: str, id: str, pre_merge=0):
        self.key = key
        self.id = id
        self.pre_merge = pre_merge
    
    def GetKey(self):
        return self.key
    
    def GetID(self):
        return self.id
    
    def GetPreMerge(self):
        return self.pre_merge
    
    def SetKey(self, key: str):
        self.key = key

    def SetID(self, id):
        self.id = id

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value
    
    def GetValue(self):
        return self.value
    
    def SetValue(self, value: str):
        self.value = value

class KVServer:
    def __init__(self, cfg, srvid):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.srvid = srvid

        # Your definitions here.
        self.kv_store = {}
        self.handled_requests = deque(maxlen=10000) # Only store last 10,000 requests

    def IsIDNew(self, id):
        if id in self.handled_requests:
            return False
        else:
            self.handled_requests.append(id)
            # id's will be removed automatically once we hit 10,000 stored
            return True
        
    def GetPrimaryID(self, key):
        try:
            primary_server_id = int(key) % self.cfg.nservers
        except ValueError:  # Handle non-numeric key values
            primary_server_id = int(ord(key)) % self.cfg.nservers
        
        return primary_server_id
        
    def GetPreferenceList(self, key):
        primary_server_id = self.GetPrimaryID(key)
        preference_list = []
        for node_id in range(self.cfg.nservers):
            for shard_cnt in range(self.cfg.nreplicas):
                if (node_id == ((primary_server_id + shard_cnt) % self.cfg.nservers)):
                    preference_list.append(node_id)
        return preference_list

    def Get(self, args: GetArgs):
        reply = GetReply(None)
        merge_reply = None
        merge_reply_list = []

        # Reject keys that aren't in my shard
        # Create the preference list
        preference_list = self.GetPreferenceList(args.GetKey())
        if self.srvid not in preference_list:
            raise KeyError("Key '{}' not in shard!".format(args.GetKey()))

        # If this node got the GET req from a client, attempt a merge (otherwise just send result)
        # This prevents recursive GET calls
        if (args.GetPreMerge() and (self.cfg.nreplicas != 0)):
            # Grab key,value pair from all replicas
            if args.GetKey() in self.kv_store:  # Add value to merge list if it exists in kv_store
                merge_reply_list.append(self.kv_store[args.GetKey()])
            else:   # Else add the tuple euivalent to a Null
                merge_reply_list.append(("", 0, args.GetID()))
            for next_server_id in preference_list:
                if (next_server_id != self.srvid):
                    try:
                        merge_reply = self.cfg.kvservers[next_server_id].Get(GetArgs(args.GetKey(), args.GetID()))
                    except TimeoutError:
                        print("ERROR: Internal server MERGE failure")
                    merge_reply_list.append(merge_reply)

            # Determine most recent key,value pair
            most_recent_value = merge_reply_list[0]
            for possible_value in merge_reply_list:
                if (possible_value[1] > most_recent_value[1]):
                    most_recent_value = possible_value

            with self.mu:
                # Overwrite this server's value with most recent value so update is on up-to-date values
                if (most_recent_value[1] != 0):  # Only overwite if one of the servers has a non-Null value
                    self.kv_store[args.GetKey()] = most_recent_value

        # If key exists, return associated value, else return ""
        if args.GetKey() in self.kv_store:
            reply.SetValue(self.kv_store[args.GetKey()])
        else:
            # Send with timestamp of 0 so that this value will never be used as the most up-to-date
            reply.SetValue(("", 0, args.GetID()))

        if debugging:
            print("Performed GET with key={} value={} id={} on server={}".format(args.GetKey(), reply.GetValue(), args.GetID(), self.srvid))

        return reply.GetValue()

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply(None)

        if debugging:
            print("Attempting PUT with key={} value={} id={} on server={}".format(args.GetKey(), args.GetValue(), args.GetID(), self.srvid))
        
        # Check if request is duplicate
        if not self.IsIDNew(args.GetID()):
            if debugging:
                print("ERROR: Duplicate PUT requested with id={}".format(args.GetID()))
            reply.SetValue("")
            return reply.GetValue()
        
        preference_list = self.GetPreferenceList(args.GetKey())

        # Overwrite (or create) key with associated value
        with self.mu:   # Perform PUT only with lock acquired
            self.kv_store[args.GetKey()] = (args.GetValue(), time.time(), args.GetID())
        
        reply.SetValue(args.GetValue()) # Fill reply with value for completeness
        
        # If this node is the primary server for this key
        # forward request to the other nodes in the preference list
        # TODO: Might need to allow downstream nodes to replicate downstream from themselves
        if (self.srvid == self.GetPrimaryID(args.GetKey())):
            for next_server_id in preference_list:
                if (next_server_id != self.srvid):
                    for attempt in range(5):
                        try:
                            internal_reply = self.cfg.kvservers[next_server_id].Put(args)
                        except TimeoutError:
                            if debugging:
                                print("Error: Failed PUT forwarding. Retrying...")
                            continue
                        else:
                            if debugging:
                                print("Success: PUT req received {}".format(internal_reply))
                            break
                    else:
                        if debugging:
                            print("ERROR: All PUT forwarding failed!")
        
        return reply.GetValue()

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)
        internal_reply = None

        if debugging:
            print("Attempting APPEND with key={} value={} id={} on server={}".format(args.GetKey(), args.GetValue(), args.GetID(), self.srvid))

        preference_list = self.GetPreferenceList(args.GetKey())

        # Check if request is duplicate
        if not self.IsIDNew(args.GetID()):
            if debugging:
                print("ERROR: Duplicate found id={}".format(args.GetID()))
            # We must remove the previously appended substring from response
            reply.SetValue(self.kv_store[args.GetKey()][0].replace(args.GetValue(), ""))
            return reply.GetValue()

        # Get the most up-to-date key,value pair from all replicas
        internal_reply = self.Get(GetArgs(args.GetKey(), None, 1))
        
        reply.SetValue(internal_reply[0].replace(args.GetValue(), ""))    # fill reply with old value

        with self.mu:   # Perform APPEND after only with lock acquired
            # Update the return value and the key,value storage
            if (internal_reply[2] != args.GetID()): # Only update if we haven't seen this APPEND req before
                self.kv_store[args.GetKey()] = (internal_reply[0]+args.GetValue(), time.time(), args.GetID()) # append new value to old value

        # If this node is the primary server for this key
        # forward request to the other nodes in the preference list
        # TODO: Might need to allow downstream nodes to replicate downstream from themselves
        if (self.srvid == self.GetPrimaryID(args.GetKey())):
            for next_server_id in preference_list:
                if (next_server_id != self.srvid):
                    for attempt in range(5):
                        try:
                            internal_reply = self.cfg.kvservers[next_server_id].Append(args)
                        except TimeoutError:
                            if debugging:
                                print("Error: Failed APPEND forwarding. Retrying...")
                            continue
                        else:
                            if debugging:
                                print("Success: APPEND req received {}".format(internal_reply))
                            break
                    else:
                        if debugging:
                            print("ERROR: All APPEND forwarding failed!")

        return reply.GetValue()