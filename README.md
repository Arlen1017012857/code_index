# Code Indexer

一个用于代码库索引和智能搜索的工具，帮助开发者更好地理解和导航大型代码库。

## 项目灵感

本项目的构思来源于以下优秀项目和文档：
- [Aider RepoMap](https://aider.chat/docs/repomap.html)
- [Cursor Codebase Indexing](https://www.cursor.com/security#codebase-indexing)

部分代码实现参考了 [code-indexer-loop](https://github.com/definitive-io/code-indexer-loop/tree/main) 项目。

## 主要功能

- **代码库索引**：使用 Merkle Tree 结构高效地索引和追踪代码库变化
- **语义搜索**：基于向量数据库的智能代码搜索
- **上下文感知**：智能分析代码结构，提供相关上下文信息
- **增量更新**：高效的代码库更新机制，只处理发生变化的文件

## 核心组件

- `MerkleTree`: 用于高效地追踪文件系统变化
- `CodeIndexer`: 负责代码解析和索引创建
- `RAGRetriever`: 实现基于检索增强生成的代码搜索功能

## 技术特点

- 使用 Merkle Tree 进行高效的文件系统变化追踪
- 采用向量数据库进行语义相似度搜索
- 支持增量更新，提高索引效率
- 智能代码解析和上下文提取

## 使用场景

- 大型代码库的智能导航
- 代码搜索和理解
- 代码库变更追踪
- 相似代码片段查找

## 开发中的功能

- [ ] 多语言支持优化
- [ ] 搜索结果排序优化
- [ ] 更多代码分析功能
- [ ] 性能优化

## 致谢

感谢以下项目和团队的启发：
- Aider 团队
- Cursor 团队
- Definitive.io 团队

## License

MIT License
