import os
import random
import math
import time
import threading
import unittest
import logging
import base64

from labrpc.labrpc import Network, Service, Server
from client import Clerk
from server import KVServer

def randstring(n):
    b = os.urandom(2 * n)
    s = base64.urlsafe_b64encode(b).decode('utf-8')
    return s[:n]

def make_seed():
    max_val = 1 << 62
    bigx = random.randint(0, max_val)
    return bigx

class Config:
    def __init__(self, t: unittest.TestCase):
        self.mu = threading.Lock()
        self.t = t
        self.net = Network()
        self.nservers = 0
        self.kvservers = None
        self.running_servers = set()
        self.clerks = {}
        self.start = time.time()
        self.t0 = None
        self.rpcs0 = 0
        self.ops = 0
        self.nreplicas = 1

    def cleanup(self):
        with self.mu:
            self.net.cleanup()

    def make_client(self):
        with self.mu:
            endnames = [randstring(20) for i in range(self.nservers)]
            ends = [self.net.make_end(endname) for endname in endnames]
            for srvid in range(self.nservers):
                self.net.connect(endnames[srvid], srvid)
            ck = Clerk(ends, self)
            self.clerks[ck] = endnames
            self.connect_client_unlocked(ck)
        return ck

    def delete_client(self, ck):
        with self.mu:
            for v in self.clerks[ck]:
                self.net.delete_end(v)
            del self.clerks[ck]

    def connect_client_unlocked(self, ck):
        endnames = self.clerks[ck]
        for srvid in range(self.nservers):
            self.net.enable(endnames[srvid], srvid in self.running_servers)

    def connect_client(self, ck):
        with self.mu:
            self.connect_client_unlocked(ck)

    def start_cluster(self, nservers):
        self.nservers = nservers
        self.kvservers = [None] * nservers
        for srvid in range(nservers):
            self.kvservers[srvid] = KVServer(self, srvid)
            kvsvc = Service(self.kvservers[srvid])
            srv = Server()
            srv.add_service(kvsvc)
            self.net.add_server(srvid, srv)
            self.running_servers.add(srvid)

    def stop_server(self, srvid):
        with self.mu:
            if srvid not in self.running_servers:
                return
            for ck in self.clerks.keys():
                endnames = self.clerks[ck]
                assert srvid < len(endnames)
                self.net.enable(endnames[srvid], False)
            self.running_servers.remove(srvid)

    def start_server(self, srvid):
        with self.mu:
            if srvid in self.running_servers:
                return
            for ck in self.clerks.keys():
                endnames = self.clerks[ck]
                assert srvid < len(endnames)
                self.net.enable(endnames[srvid], True)
            self.running_servers.add(srvid)

    def begin(self, description):
        print(f"{description} ...\n")
        self.t0 = time.time()
        self.rpcs0 = self.rpc_total()
        with self.mu:
            self.ops = 0

    def op(self):
        with self.mu:
            self.ops += 1

    def rpc_total(self):
        return self.net.get_total_count()

    def end(self):
        if self.t.defaultTestResult().wasSuccessful():
            t = time.time() - self.t0
            nrpc = self.rpc_total() - self.rpcs0
            with self.mu:
                ops = self.ops
            print("  ... Passed --")
            print(f" t {t} nrpc {nrpc} ops {ops}\n")

def make_single_config(t, unreliable):
    cfg = Config(t)
    cfg.clerks = {}
    cfg.start = time.time()
    cfg.start_cluster(1)
    cfg.net.reliable(not unreliable)
    return cfg

def make_shard_config(t, nshards, nreplicas, unreliable):
    cfg = Config(t)
    cfg.clerks = {}
    cfg.start = time.time()
    cfg.start_cluster(nshards)
    cfg.nreplicas = nreplicas
    cfg.net.reliable(not unreliable)
    return cfg
