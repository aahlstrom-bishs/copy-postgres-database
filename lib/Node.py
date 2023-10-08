from typing import Union, List
import json
import numpy as np

from UsefulFunctions import flatten_list

# FIXME running out of RAM : use numpy to make this smaller and faster



""" Example usage:

# NORMAL

root = Node(None, None, None)
root.set_max_distance(4) # default = 20
root.set_min_search_distance(1) # default = 5
root.add_path("hello world", "ardvark")
root.search_path("lo w")



# ADVANCED

root = Node(None, None, None)
root.set_max_distance(6) # default = 20
root.set_min_search_distance(1) # default = 5
a = root.add_child("a", "ardvark")
b = a.add_child("b", "butterfly")
b = a.add_child("b", "butter")
c = b.add_child("c", "cadmire")
d = c.add_child("d", "dinosaur")
d_test = c.add_child("d", "dragonfly")

print(b.to_dict())
print(d.to_dict())
print("d is d_test? ", d is d_test)
"""

num_flags = 650
bit_flags = np.zeros(num_flags, dtype=np.uint64)

key_map = {}
key_counter = 0
value_map = {}
value_counter = 0

class Node:
    # for some reason, overloading __init__ isn't working...
    def __init__(self, parent: "Node" = None, key: str = None, value: str = None):
        global key_map
        global key_counter
        global value_map
        global value_counter

        if (parent == None):
            assert(key == None and value == None)
            self.values = []
            self.distance = 0
            self.max_distance = 20
            self.min_search_distance = 5
        else:
            assert(key != None and value != None)
            if not value in value_map:
                value_map[value] = value_counter
                value_counter += 1
            self.distance = parent.distance + 1
            self.min_search_distance = parent.min_search_distance
            self.values = [] if (self.distance < self.min_search_distance) else [value_map[value]]
            self.max_distance = parent.max_distance
        
        if (not key in key_map):
            # print("adding key", key, "to key_map")
            key_map[key] = key_counter
            key_counter += 1
        self.key = key_map[key]
        self.parent = parent
        self.children = []
        self.visited = False

    def set_max_distance(self, max_distance: int):
        self.max_distance = max_distance
    
    def set_min_search_distance(self, min_search_distance: int):
        self.min_search_distance = min_search_distance

    def get_values(self):
        reverse_value_map = {v: k for k, v in value_map.items()}
        return [reverse_value_map[value_index] for value_index in self.values]
    
    # adds a child node to this node
    # key must be a single character
    # must use this function to add children, otherwise get_long_value can result in infinite loop
    def add_child(self, _key: str, _value: str):
        global key_map
        global key_counter
        global value_map
        global value_counter
        # print("\nkey: ", _key, "value: ", _value)
        
        if (self.distance >= self.max_distance):
            return None;
        assert(len(_key) == 1)

        keys = [child.key for child in self.children]
        if (_key in key_map and key_map[_key] in keys):
            # print("   -- found matching key")
            child = self.children[keys.index(key_map[_key])]
            if not _value in value_map:
                # print("  -- adding value to value_map")
                value_map[_value] = value_counter
                value_counter += 1
            value_index = value_map[_value]
            if (self.distance >= self.min_search_distance
                and value_index != None
                and value_index not in child.values 
            ):
                # print("   -- adding value to child")
                child.values.append(value_index)
            # else:
                # print(f"  -- value not added to child... {value_index} : {child.values}")
            return child
        else:
            # print("   -- creating new child")
            self.children.append(Node(self, _key, _value))
            return self.children[-1]

    def get_children(self):
        return self.children

    def get_key(self):
        return self.key
    
    # to be called on the root node
    def add_path(self, path: str, value: str):
        if (self.parent != None):
            raise Exception("add_path() must be called on the root node")
        if (path == None or len(path) <= 0 or value == None):
            raise Exception("path and value must be non-empty strings")
        
        for i, _ in enumerate(path):
            # print("\nroot: ---------------------------")
            self.forge_path(path[i:], value)
    
    def forge_path(self, path: str, value: str):
        if (len(path) > 0):
                child = self.add_child(path[0], value)
                if (child != None):
                    child.forge_path(path[1:], value) 

    # returns the full path from the root node to this node
    # so long as only add_child() is used to add child nodes, this will not result in an infinite loop
    def get_path_string(self):
        path = self.get_path()
        reverse_key_map = {v: k for k, v in key_map.items()}
        path_string = [reverse_key_map[key] for key in path]
        return "".join([str(s) for s in path_string if s != None])
    
    def get_path(self):
        return flatten_list(self.get_path_recursive())

    def get_path_recursive(self):
        key_index = self.key
        if (self.parent == None):
            return [key_index]
        path = self.parent.get_path_recursive()
        path.append(key_index)
        return path
    
    def search_path_get_values(self, path: str):
        return list(set(flatten_list([node.get_values() for node in self.search_path(path)])))

    def search_path(self, path: str):
        if (self.parent != None):
            raise Exception("search_path() must be called on the root node")
        if (path == None or len(path) <= 0):
            raise Exception("path must be a non-empty string")

        return self.find_nodes_with_matching_path(path, self)
    
    def find_nodes_with_matching_path(self, path: str, root: "Node"):
        if (len(path) <= 0):
            return self
        matches = []
        # if (len(self.children) == 0):
        #     recursive_matches = [
        #         node 
        #         for node 
        #         in root.search_path(path)
        #         if any(match in self.values for match in node.values)
        #     ]

        for child in self.children:
            if (child.key == key_map.get(path[0], -1)):
                matches.append(child.find_nodes_with_matching_path(path[1:], root))
        return flatten_list(matches)
    

    def get_distance(self):
        return self.distance
    
    def get_parent(self):
        return self.parent
    
    def set_visited(self, visited: bool):
        self.visited = visited

    def is_visited(self):
        return self.visited
    
    def to_dict(self):
        global key_map
        global value_map
        global key_counter
        global value_counter

        path = self.get_path()
        return {
            "max_distance": self.max_distance,
            "min_search_distance": self.min_search_distance,
            "key_map": key_map,
            "value_map": value_map,
            "key_counter": key_counter,
            "value_counter": value_counter,
            "distance: ": self.distance,
            "key": self.key,
            "path": "" if path == None else path,
            "values": self.values,
            "num_children": len(self.children),
            "children": [child.to_dict_recursive(path) for child in self.children]
        }
    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)
    
    def to_dict_recursive(self, _path: List[int] = None):
        if _path == None:
            path = self.get_path()
        else:
            path = _path + [self.key]
        return {
            "distance": self.distance,
            "key": self.key,
            "path": path,
            "values": self.values,
            "num_children": len(self.children),
            "children": [child.to_dict_recursive(path) for child in self.children]
        }
    
    def to_list(self, master_list: List[List] = None):
        # parent, key, values
        if master_list == None:
            master_list = []
        _parent_index = None if self.parent == None else self.parent.index
        master_list.append([_parent_index, self.key, self.values])
        self.index = len(master_list) - 1
        for child in self.children:
            child.to_list(master_list)

        return master_list

    def to_small_file(self, filename: str = "state_machine"):
        output = json.dumps({
            "max_distance": self.max_distance,
            "min_search_distance": self.min_search_distance,
            "key_map": key_map,
            "value_map": value_map,
            "key_counter": key_counter,
            "value_counter": value_counter,
            "data": self.to_list()
        })
        with open(f'outputs/{filename}.json', 'w') as f:
            f.write(output)
        
    def load_file(self, filename: str = "state_machine"):
        with open(f'outputs/{filename}.json', 'r') as f:
            data = json.load(f)
        self.max_distance = data["max_distance"]
        self.min_search_distance = data["min_search_distance"]
        global key_map
        global value_map
        global key_counter
        global value_counter
        key_map = data["key_map"]
        value_map = data["value_map"]
        key_counter = data["key_counter"]
        value_counter = data["value_counter"]
        self.build_from_list(data["data"])

    def build_from_list(self, data: List[List]):
        self.parent = data[0][0]
        self.key = data[0][1]
        self.values = data[0][2]
        self.children = []
        for child_data in data[1:]:
            child = Node()
            child.parent = self
            child.build_from_list_recursive(child_data)

    def build_from_dict(file_name: str):
        with open(file_name, "r") as f:
            data = json.load(f)
        root = Node()
        root.max_distance = data.get("max_distance", 20)
        key_map = data.get("key_map", {})
        value_map = data.get("value_map", {})
        key_counter = data.get("key_counter", {})
        value_counter = data.get("value_counter", {})

        nodes = [
            {
                "parent": node[0],
                "key": node[1],
                "values": node[2],
                "children": []
            }
            for node
            in data["data"]
        ]
        for (index, node) in enumerate(nodes):
            if node["parent"] != None:
                nodes[node["parent"]]["children"].append(index)
        
        # FIXME: I'm too tired to think... 
        return root
    
    def build_from_dict_recursive(self, data: dict):
        self.max_distance = data.get("max_distance", 10_000)
        self.distance = data["distance"]
        self.key = data["key"]
        self.values = data["values"]
        self.children = []
        for child_data in data["children"]:
            child = Node()
            child.parent = self
            child.build_from_dict_recursive(child_data)
            self.children.append(child)