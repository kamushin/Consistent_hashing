from ConsistentHashing import Hash
import unittest
from unittest import TestCase

class TestConsistentHashing(TestCase):
    def setUp(self):
        self.serverList = ['127.0.0.1:8889', '127.0.0.1:9001', '127.0.0.1:9003']
        self.replicas = 5
        self.ring = Hash(self.serverList, replicas=self.replicas)

    def tearDown(self):
        self.ring = None

    def test_init(self):
        nodes = self.ring.allnodes()
        for node in self.serverList:
            self.assertTrue(node in nodes)
            self.assertTrue(len(nodes[node]) == self.replicas)
        
    def test_add(self):
        node = '127.0.0.1:9000'
        self.ring.add_node(node)

        nodes = self.ring.allnodes()

        self.assertTrue(node in nodes)
        self.assertTrue(len(nodes[node]) == self.replicas)

    def test_dup_add(self):
        node = '127.0.0.1:9000'
        self.ring.add_node(node)
        self.ring.add_node(node)

        nodes = self.ring.allnodes()

        self.assertTrue(node in nodes)
        self.assertTrue(len(nodes[node]) == self.replicas)

    def test_remove(self):
        node = '127.0.0.1:9000'
        self.ring.add_node(node)
        self.ring.remove_node(node)

        nodes = self.ring.allnodes()

        self.assertTrue(node not in nodes)


    def test_get_node(self):
        url = 'www.foo.bar.com'

        self.assertTrue(self.ring.get_node(url) in self.serverList)

if __name__ == '__main__':
    unittest.main()
    
