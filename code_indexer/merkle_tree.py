import hashlib
from typing import Dict, Optional, List
import os

class MerkleNode:
    def __init__(self, hash_value: str, is_file: bool = False):
        self.hash_value = hash_value
        self.is_file = is_file
        self.children: Dict[str, MerkleNode] = {}

class MerkleTree:
    def __init__(self, root_path: str):
        self.root_path = root_path
        self.root = MerkleNode("")
        self.build_tree()
        
    def compute_file_hash(self, file_path: str) -> str:
        """计算文件的SHA-256哈希值"""
        with open(file_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
            
    def compute_directory_hash(self, children_hashes: List[str]) -> str:
        """计算目录的哈希值（基于其子节点的哈希值）"""
        combined = ''.join(sorted(children_hashes))
        return hashlib.sha256(combined.encode()).hexdigest()
        
    def build_tree(self):
        """构建整个Merkle树"""
        self._build_node(self.root_path, self.root)
        
    def _build_node(self, path: str, node: MerkleNode) -> str:
        """递归构建树节点"""
        if os.path.isfile(path):
            hash_value = self.compute_file_hash(path)
            node.hash_value = hash_value
            node.is_file = True
            return hash_value
            
        children_hashes = []
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            child_node = MerkleNode("")
            node.children[item] = child_node
            child_hash = self._build_node(item_path, child_node)
            children_hashes.append(child_hash)
            
        node.hash_value = self.compute_directory_hash(children_hashes)
        return node.hash_value
        
    def get_changes(self, other_tree: 'MerkleTree') -> List[str]:
        """比较两个Merkle树，返回发生变化的文件路径"""
        changes = []
        self._compare_nodes(self.root_path, self.root, other_tree.root, changes)
        return changes
        
    def _compare_nodes(self, path: str, node1: MerkleNode, node2: MerkleNode, changes: List[str]):
        """递归比较两个节点"""
        if node1.hash_value != node2.hash_value:
            if node1.is_file:
                changes.append(path)
            else:
                # 比较子节点
                all_children = set(node1.children.keys()) | set(node2.children.keys())
                for child in all_children:
                    child_path = os.path.join(path, child)
                    child1 = node1.children.get(child, MerkleNode(""))
                    child2 = node2.children.get(child, MerkleNode(""))
                    self._compare_nodes(child_path, child1, child2, changes)
                    
    def update_file(self, file_path: str):
        """更新单个文件的哈希值"""
        if not os.path.exists(file_path):
            return
            
        relative_path = os.path.relpath(file_path, self.root_path)
        path_parts = relative_path.split(os.sep)
        
        current_node = self.root
        current_path = self.root_path
        
        # 遍历路径更新节点
        for part in path_parts[:-1]:
            current_path = os.path.join(current_path, part)
            if part not in current_node.children:
                current_node.children[part] = MerkleNode("")
            current_node = current_node.children[part]
            
        # 更新文件节点
        file_name = path_parts[-1]
        if file_name not in current_node.children:
            current_node.children[file_name] = MerkleNode("")
        file_node = current_node.children[file_name]
        file_node.hash_value = self.compute_file_hash(file_path)
        file_node.is_file = True
        
        # 更新父目录的哈希值
        self._update_parent_hashes(path_parts[:-1], self.root)

    def get_node_hash(self, file_path: str) -> Optional[str]:
        """获取指定文件或目录的哈希值"""
        if not os.path.exists(file_path):
            return None
            
        relative_path = os.path.relpath(file_path, self.root_path)
        path_parts = relative_path.split(os.sep)
        
        current_node = self.root
        for part in path_parts:
            if part not in current_node.children:
                return None
            current_node = current_node.children[part]
            
        return current_node.hash_value
        
    def _update_parent_hashes(self, path_parts: List[str], node: MerkleNode):
        """更新父目录的哈希值"""
        if not path_parts:
            return
            
        current_node = node
        for part in path_parts:
            if part not in current_node.children:
                return
            current_node = current_node.children[part]
            
            # 重新计算目录哈希值
            children_hashes = [child.hash_value for child in current_node.children.values()]
            current_node.hash_value = self.compute_directory_hash(children_hashes)
            
    def get_all_files(self) -> List[str]:
        """获取所有文件路径"""
        files = []
        self._collect_files(self.root_path, self.root, files)
        return files
        
    def _collect_files(self, path: str, node: MerkleNode, files: List[str]):
        """递归收集文件路径"""
        if node.is_file:
            files.append(path)
        else:
            for name, child in node.children.items():
                child_path = os.path.join(path, name)
                self._collect_files(child_path, child, files)
