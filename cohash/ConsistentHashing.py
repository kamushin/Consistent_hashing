from hashlib import md5
from bisect import bisect

class Hash(object):

    def __init__(self, nodes=[], replicas=3):
        '''
            nodes is a list of server name (always be ip with port like 127.0.0.1:8001)
            replicas is the replica server number of a real server
        '''
        self.replicas =replicas

        self.key_map = {}
        self.nodes = {}
        self.ring = []
    
        for node in nodes:
            self.add_node(node)


    def add_node(self, node):
        '''
            add node to ring, ignore the duplicate node
        ''' 
        if node in self.nodes:
            return 
        else:
            self.nodes[node] = 1

        for i in range(self.replicas):
            key = self._simple_hash("%s%s" % (node, i))

            self.key_map[key] = node
            self.ring.append(key)

        self.ring.sort()


    def remove_node(self, node):
        '''
            remove node from ring, if exist
        '''
        if node not in self.nodes:
            return 
        else:
            del self.nodes[node]

        for i in range(self.replicas):
            key = self._simple_hash("%s%s" % (node, i))

            del self.key_map[key] 
            self.ring.remove(key)

        self.ring.sort() 

    def allnodes(self):
        '''
            return all nodes in ring
        '''
        node_map = {}
        for k,v in self.key_map.items():
            if v not in node_map:
                node_map[v] = []

            node_map[v].append(k)

        return node_map


    def get_node(self, string):
        '''
            return a load node for a string(url, name , etc.)
            if ring is empty, return None
        '''
        if not self.ring:
            return None

        key = self._simple_hash(string)
        
        pos = bisect(self.ring, key)

        if pos == len(self.ring):
            return self.key_map[self.ring[0]]
        else:
            return self.key_map[self.ring[pos]]


    def _simple_hash(self, node):
        if isinstance(node, str):
            string = node.encode()
        elif isinstance(node, bytes):
            string = node
        else:
            raise TypeError("node type error")

        m = md5(string)
        return int(m.hexdigest(), 16)
        

