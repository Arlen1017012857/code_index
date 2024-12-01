import os
from typing import List, Dict, Tuple
import numpy as np
from fastembed import TextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    SparseVectorParams,
    SparseIndexParams,
    PointStruct,
    SparseVector,
    SearchRequest,
    NamedVector,
    NamedSparseVector,
)

from .merkle_tree import MerkleTree
from .code_splitter import CodeSplitter
from .constants import EXTENSION_TO_TREE_SITTER_LANGUAGE

class HybridCodeSearch:
    def __init__(
        self,
        root_path: str,
        sparse_model_name: str = "prithvida/Splade_PP_en_v1",
        dense_model_name: str = "BAAI/bge-large-en-v1.5",
    ):
        self.root_path = root_path
        self.merkle_tree = MerkleTree(root_path)
        
        # Initialize embedding models
        self.sparse_model = SparseTextEmbedding(model_name=sparse_model_name)
        self.dense_model = TextEmbedding(model_name=dense_model_name)
        
        # Initialize code splitters for different languages
        self.code_splitters = {}
        
        # Initialize Qdrant client
        self.client = QdrantClient(":memory:")
        self._create_collection()
        
    def _create_collection(self):
        """创建Qdrant集合用于存储代码嵌入向量"""
        self.client.create_collection(
            "code-index",
            vectors_config={
                "text-dense": VectorParams(
                    size=1024,
                    distance=Distance.COSINE,
                )
            },
            sparse_vectors_config={
                "text-sparse": SparseVectorParams(
                    index=SparseIndexParams(
                        on_disk=False,
                    )
                )
            },
        )
        
    def _get_code_splitter(self, file_path: str) -> CodeSplitter:
        """获取对应语言的代码分割器"""
        ext = os.path.splitext(file_path)[1]
        if ext not in EXTENSION_TO_TREE_SITTER_LANGUAGE:
            raise ValueError(f"Unsupported file extension: {ext}")
            
        language = EXTENSION_TO_TREE_SITTER_LANGUAGE[ext]
        if language not in self.code_splitters:
            self.code_splitters[language] = CodeSplitter(language)
            
        return self.code_splitters[language]
        
    def index_files(self):
        """索引所有文件"""
        files = self.merkle_tree.get_all_files()
        points = []
        
        for idx, file_path in enumerate(files):
            try:
                # 获取文件扩展名
                ext = os.path.splitext(file_path)[1]
                if ext not in EXTENSION_TO_TREE_SITTER_LANGUAGE:
                    continue
                    
                # 获取代码分割器
                splitter = self._get_code_splitter(file_path)
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    relative_path = os.path.relpath(file_path, self.root_path)
                    
                    # 分割代码
                    chunks = splitter.split_text_with_metadata(content)
                    
                    # 为每个代码块创建索引点
                    for chunk_idx, (chunk_text, metadata) in enumerate(chunks):
                        # 生成稀疏和密集嵌入向量
                        sparse_vector = list(self.sparse_model.embed([chunk_text]))[0]
                        dense_vector = list(self.dense_model.embed([chunk_text]))[0]
                        
                        # 创建索引点
                        point = PointStruct(
                            id=f"{idx}_{chunk_idx}",
                            payload={
                                "path": relative_path,
                                "hash": self.merkle_tree.get_node_hash(file_path),
                                "chunk_text": chunk_text,
                                "metadata": metadata.__dict__,
                            },
                            vector={
                                "text-sparse": SparseVector(
                                    indices=sparse_vector.indices.tolist(),
                                    values=sparse_vector.values.tolist(),
                                ),
                                "text-dense": dense_vector.tolist(),
                            },
                        )
                        points.append(point)
            except Exception as e:
                print(f"Error indexing file {file_path}: {str(e)}")
                
        # 批量上传索引点
        if points:
            self.client.upsert("code-index", points)
            
    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """混合搜索实现"""
        # 生成查询向量
        query_sparse = list(self.sparse_model.embed([query]))[0]
        query_dense = list(self.dense_model.embed([query]))[0]
        
        # 执行混合搜索
        search_results = self.client.search_batch(
            collection_name="code-index",
            requests=[
                SearchRequest(
                    vector=NamedVector(
                        name="text-dense",
                        vector=query_dense.tolist(),
                    ),
                    limit=limit,
                    with_payload=True,
                ),
                SearchRequest(
                    vector=NamedSparseVector(
                        name="text-sparse",
                        vector=SparseVector(
                            indices=query_sparse.indices.tolist(),
                            values=query_sparse.values.tolist(),
                        ),
                    ),
                    limit=limit,
                    with_payload=True,
                ),
            ],
        )
        
        # 使用RRF(Reciprocal Rank Fusion)合并结果
        dense_results, sparse_results = search_results
        combined_results = self._combine_results(dense_results, sparse_results)
        
        return [
            {
                "path": result.payload["path"],
                "score": score,
                "hash": result.payload["hash"],
            }
            for result, score in combined_results
        ]
        
    def _combine_results(self, dense_results, sparse_results, alpha: float = 60):
        """使用RRF算法合并搜索结果"""
        # 创建排名字典
        ranks = {}
        
        # 处理密集向量结果
        for rank, result in enumerate(dense_results, 1):
            if result.id not in ranks:
                ranks[result.id] = {"result": result, "dense_rank": rank, "sparse_rank": float("inf")}
            else:
                ranks[result.id]["dense_rank"] = rank
                
        # 处理稀疏向量结果
        for rank, result in enumerate(sparse_results, 1):
            if result.id not in ranks:
                ranks[result.id] = {"result": result, "dense_rank": float("inf"), "sparse_rank": rank}
            else:
                ranks[result.id]["sparse_rank"] = rank
                
        # 计算RRF分数
        results_with_scores = []
        for item in ranks.values():
            rrf_score = (1 / (alpha + item["dense_rank"])) + (1 / (alpha + item["sparse_rank"]))
            results_with_scores.append((item["result"], rrf_score))
            
        # 按分数排序
        return sorted(results_with_scores, key=lambda x: x[1], reverse=True)
        
    def update_index(self, file_path: str):
        """更新单个文件的索引"""
        if not os.path.exists(file_path):
            return
            
        # 获取文件扩展名
        ext = os.path.splitext(file_path)[1]
        if ext not in EXTENSION_TO_TREE_SITTER_LANGUAGE:
            return
            
        # 获取代码分割器
        splitter = self._get_code_splitter(file_path)
        
        # 更新Merkle树
        self.merkle_tree.update_file(file_path)
        
        # 更新向量索引
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            relative_path = os.path.relpath(file_path, self.root_path)
            
            # 分割代码
            chunks = splitter.split_text_with_metadata(content)
            
            # 删除文件的所有现有索引点
            self.client.delete(
                collection_name="code-index",
                points_selector={"must": [{"key": "path", "match": {"value": relative_path}}]},
            )
            
            points = []
            for chunk_idx, (chunk_text, metadata) in enumerate(chunks):
                # 生成新的嵌入向量
                sparse_vector = list(self.sparse_model.embed([chunk_text]))[0]
                dense_vector = list(self.dense_model.embed([chunk_text]))[0]
                
                # 创建新的索引点
                point = PointStruct(
                    id=f"{relative_path}_{chunk_idx}",
                    payload={
                        "path": relative_path,
                        "hash": self.merkle_tree.get_node_hash(file_path),
                        "chunk_text": chunk_text,
                        "metadata": metadata.__dict__,
                    },
                    vector={
                        "text-sparse": SparseVector(
                            indices=sparse_vector.indices.tolist(),
                            values=sparse_vector.values.tolist(),
                        ),
                        "text-dense": dense_vector.tolist(),
                    },
                )
                points.append(point)
                
            # 批量上传新的索引点
            if points:
                self.client.upsert("code-index", points)
