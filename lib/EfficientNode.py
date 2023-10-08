from typing import List
import numpy as np
import string
import json
import re

import sys
sys.path.append('lib')

from BitFlags import BitFlags

def build_map(values: List[str]):
    values = np.array(values)
    values = np.unique(values)
    values = np.sort(values)
    values = dict(zip(values, [i for i in range(len(values))]))
    return values

def get_reverse_map(values: dict):
    return dict(zip(values.values(), values.keys()))

def add_entry_to_map(values: dict, value: str):
    if value not in values:
        values[value] = len(values)
    return values

def process_string(input_string):
    # Use a regular expression to match non-alphanumeric characters
    pattern = r'[^a-zA-Z0-9]'
    # Replace the non-alphanumeric characters with an empty string
    result_string = re.sub(pattern, '', input_string)
    return result_string.lower()


class Node:
    def __init__(self, values: List[str]):
        self.key_map = {}
        self.value_map = build_map(values)
        self.reverse_values_map = get_reverse_map(self.value_map)
        self.num_flags = len(values)
        self.node_type = np.dtype([
            ('parent', np.uint64), 
            ('key', np.uint8), 
            ('values', np.uint64, (np.uint8(np.ceil(self.num_flags/64)),))
        ])
        self.nodes = {
            'parents': [None],
            'keys': [None],
            'values': [None],
            'children': [[]]
        }
    def add_node(self, parent_index: np.uint64, key: str, value: str):
        key = process_string(key)
        if key == "":
            raise Exception("key must be alphanumeric")
        
        if not value in self.value_map:
            raise Exception("Value not found in map: " + value)

        if not key in self.key_map:
            add_entry_to_map(self.key_map, key)
        if (parent_index == None):
            parent_index = 0 # root node
        for child_index in self.nodes['children'][parent_index]:
            if self.nodes['keys'][child_index] == self.key_map[key]:
                self.nodes['values'][child_index].set_bit(self.value_map[value])
                return child_index
        # match not found
        node_index = len(self.nodes['parents'])
        self.nodes['parents'].append(parent_index)
        self.nodes['keys'].append(self.key_map[key])
        self.nodes['values'].append(BitFlags(self.num_flags))
        self.nodes['children'].append([])

        self.nodes['values'][node_index].set_bit(self.value_map[value])
        self.nodes['children'][parent_index].append(node_index)
        return node_index
    
    def add_path(self, path: List[str], value: str):
        path = process_string(path)
        root_index = 0 # root node
        for (i, _) in enumerate(path):
            current_path = path[i:]
            current_parent_index = root_index
            for key in current_path:
                current_parent_index = self.add_node(current_parent_index, key, value)

    def search(self, path: str):
        path = process_string(path)
        node_index = 0 # root node
        found = False
        if any([key not in self.key_map for key in path]):
            return []
        for key in path:
            found = False
            key = self.key_map[key]
            for child_index in self.nodes['children'][node_index]:
                if self.nodes['keys'][child_index] == key:
                    node_index = child_index
                    found = True
                    break
            if not found:
                return []
        
        return [
            self.reverse_values_map.get(index, index) 
            for index 
            in self.nodes['values'][node_index].get_flags()
        ]
    
    def save(self, filename: str = 'state_machine'):
        # give root node valid values
        self.nodes['parents'][0] = 0;
        self.nodes['keys'][0] = 0;
        self.nodes['values'][0] = BitFlags(self.num_flags);
        # build memory efficient numpy array
        num_nodes = len(self.nodes['parents'])
        np_nodes = np.empty(num_nodes, dtype=self.node_type)
        print(np_nodes.dtype)
        np_nodes['parent'] = np.array(self.nodes['parents'], dtype=np.uint64)
        np_nodes['key'] = np.array(self.nodes['keys'], dtype=np.uint8)
        np_nodes['values'] = np.array([bit.flags for bit in self.nodes['values']])

        maps = {
            'key_map': self.key_map,
            'value_map': self.value_map
        }

        np.save(f'outputs/{filename}_nodes.bin', np_nodes)
        with open(f'outputs/{filename}_maps.json', 'w') as f:
            json.dump(maps, f, indent=4)
    
    def build_from_file(filename: str = 'state_machine'):
        root = Node([])
        root.load(filename)
        return root

    def load(self, filename: str = 'state_machine'):
        np_nodes = np.load(f'outputs/{filename}_nodes.bin.npy')
        with open(f'outputs/{filename}_maps.json', 'r') as f:
            maps = json.load(f)
        self.key_map = maps['key_map']
        self.value_map = maps['value_map']
        self.reverse_values_map = get_reverse_map(self.value_map)
        self.num_flags = len(self.value_map)
        self.node_type = np.dtype([
            ('parent', np.uint64), 
            ('key', np.uint8), 
            ('values', np.uint64, (np.uint8(np.ceil(self.num_flags/64)),))
        ])
        self.nodes = {
            'parents': np_nodes['parent'].tolist(),
            'keys': np_nodes['key'].tolist(),
            'values': [BitFlags.from_flags(flags) for flags in np_nodes['values']],
            'children': [[] for _ in range(len(np_nodes))]
        }
        for i in range(1, len(self.nodes['parents'])):
            self.nodes['children'][self.nodes['parents'][i]].append(i)