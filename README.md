# Gzip NDJSON Batch Log

一个用 **Python + FastAPI + SQLite** 实现的 HTTP 批处理日志服务：上传一个 gzip
流，服务端逐块解压、逐行校验 NDJSON，仅当整个 gzip 成员完整接收 **且 CRC 校验
通过** 后才把整批原子写入 SQLite；在此之前任何一行都不可查询。

## 一条命令启动（Docker Compose）

```bash
docker compose up --build
```

- 监听端口：**`8000`**（宿主 `8000` → 容器 `8000`）
- 数据持久化在命名卷 `batchlog-data`（容器内 `/data`，SQLite 数据库与暂存文件）
- 容器启动时会自动执行幂等的数据库初始化（建表、WAL 模式）
- 健康检查：<http://localhost:8000/health>，交互文档：<http://localhost:8000/docs>

停止：`Ctrl+C`（后台运行则 `docker compose down`；保留数据，加 `-v` 才删除卷）。

### 不用 Docker 的本地运行方式

```bash
pip install -r requirements.txt
python -c "from app.config import ensure_dirs; ensure_dirs(); from app import db; db.init_db()"
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

数据目录默认是 `./data`，可用环境变量 `BATCHLOG_DATA_DIR` 覆盖。

## 请求体与记录规则

- 请求体：**单个 gzip 成员**（RFC 1952，`Content-Type` 不限，例如
  `application/gzip`、`application/octet-stream`）。
- 解压后为非空 UTF-8 **NDJSON**：一行一条 JSON，行之间以单个 `\n` 分隔；
  不允许空行；最后一行可以没有结尾换行。
- 每行必须恰好包含两个非空字符串字段：`id`、`message`（不允许多字段、
  非字符串、空字符串、重复 JSON 键）。
- 同一批次内 `id` 必须唯一（不同批次之间允许相同）。
- 字节限额（均按**字节**计，超限立即停止读取并返回 `413`）：

| 限额 | 值 |
| --- | --- |
| 压缩后请求体 | 2 MiB（2,097,152 字节） |
| 解压后总大小 | 8 MiB（8,388,608 字节） |
| 单行正文（不含 `\n`） | 64 KiB（65,536 字节） |

- gzip 损坏、CRC/ISIZE 错误、流被截断、成员后存在尾随数据（含多个 gzip 成员）、
  非法记录（编码/JSON/字段/重复 id/空行）一律返回 `422`。
- 上传中断、校验失败或入库失败都不会留下任何可查询批次（落库为单事务，
  暂存文件立即删除）。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/batches` | 上传 gzip NDJSON 流；成功 `201` 返回批次 ID 与行数 |
| `GET` | `/batches?limit=&after_id=` | 批次列表（ID、行数、创建时间），按批次 ID 键集分页 |
| `GET` | `/batches/{batch_id}` | 批次元数据；未知 ID 返回 `404` |
| `GET` | `/batches/{batch_id}/rows?limit=&offset=` | 按原始顺序分页读行；未知批次 `404` |
| `GET` | `/health` | 健康检查 |

行顺序与上传的 NDJSON 完全一致（`position` 从 1 开始）。分页参数：
`limit` 1–1000（默认 100），读行接口 `offset >= 0`；响应中 `next_offset` /
`next_after_id` 为 `null` 表示已到末页。

## curl 示例

### 正常：上传并查询

```bash
# 准备两条记录并 gzip
printf '%s\n' \
  '{"id":"u1","message":"hello world"}' \
  '{"id":"u2","message":"第二条"}' > records.ndjson
gzip -c records.ndjson > records.ndjson.gz

# 上传（201 + batch_id）
curl -i -X POST http://localhost:8000/batches \
  --data-binary @records.ndjson.gz
# HTTP/1.1 201 Created
# {"batch_id":"f180da2bdd8144ae8a49fbf95c146e4b","rows":2}

BID=f180da2bdd8144ae8a49fbf95c146e4b   # 替换成上一步返回的 ID

# 批次元数据
curl http://localhost:8000/batches/$BID
# {"batch_id":"...","rows":2,"created_at":"2026-10-05T16:02:54.412Z"}

# 按原始顺序分页读行
curl "http://localhost:8000/batches/$BID/rows?limit=1&offset=0"
curl "http://localhost:8000/batches/$BID/rows?limit=1&offset=1"

# 批次列表（按批次 ID 键集分页）
curl "http://localhost:8000/batches?limit=100"
```

### 失败示例

```bash
# 1) 不是 gzip / gzip 损坏 -> 422
printf '{"id":"x","message":"y"}\n' | \
  curl -i -X POST http://localhost:8000/batches --data-binary @-
# HTTP/1.1 422 Unprocessable Entity
# {"error":"InvalidRecord","detail":"corrupt gzip stream: ... incorrect header check"}

# 2) 批内 id 重复 -> 422（且不会产生任何批次）
printf '%s\n' '{"id":"d","message":"1"}' '{"id":"d","message":"2"}' | gzip | \
  curl -i -X POST http://localhost:8000/batches --data-binary @-
# 422 {"error":"InvalidRecord","detail":"duplicate id within batch: 'd'"}

# 3) 字段非法（message 不是字符串）-> 422
printf '%s' '{"id":"d","message":123}' | gzip | \
  curl -i -X POST http://localhost:8000/batches --data-binary @-
# 422 {"error":"InvalidRecord","detail":"'message' must be a non-empty string"}

# 4) gzip 成员后有尾随数据 -> 422
{ printf '%s\n' '{"id":"d","message":"ok"}' | gzip; printf JUNK; } | \
  curl -i -X POST http://localhost:8000/batches --data-binary @-
# 422 {"error":"InvalidRecord","detail":"trailing data after gzip member"}

# 5) gzip 被截断 / CRC 错误 -> 422
# 6) 空行 -> 422 {"detail":"empty line is not allowed"}

# 7) 请求体超过 2 MiB -> 413（服务端立即停止读取并关闭连接）
curl -i -X POST http://localhost:8000/batches \
  -H "Content-Length: 2100000" --data-binary @<(head -c 2100000 /dev/zero)
# HTTP/1.1 413 Payload Too Large
# {"error":"PayloadTooLarge","detail":"content-length 2100000 exceeds 2097152 bytes"}

# 8) 未知批次 -> 404
curl -i http://localhost:8000/batches/deadbeef/rows
# HTTP/1.1 404 Not Found
# {"detail":"unknown batch id: deadbeef"}
```

## 设计要点（为什么慢上传不会互相阻塞）

- **流式接收**：ASGI 分块接收，压缩体从不整体缓冲；到达的压缩块经有界队列
  （容量 16）交给**每个请求独立的工作线程**。
- **有界解压**：`zlib.decompressobj(wbits=31)` 每次调用都设 `max_length=64KiB`；
  未消费的压缩字节（`unconsumed_tail`）作为下一次输入回喂——避免“解压炸弹”
  一次性撑出大对象，也不依赖额外空喂（CPython 在此场景下会卡死 inflate 状态机）。
- **暂存而非驻留内存**：校验通过的行直接写入每批次独立的磁盘暂存 SQLite
  （含 `UNIQUE(id)` 约束），既不累积行内容，也不维护内存 id 集合。
- **提交点可见性**：请求体结束后 `flush()` 驱动 gzip 尾部，由 zlib 完成
  CRC32/ISIZE 校验；随后在主库上用**单个事务** `ATTACH` 暂存库、插入批次与全部
  行。此前任何时刻行都不可见；任何失败回滚并删除暂存文件。
- **并发**：慢上传只会让它自己的工作线程和有界队列等待，事件循环继续接收并
  服务其他上传；所有读接口为同步函数，由 Starlette 线程池执行，不阻塞事件循环。
- SQLite 启用 WAL，短连接 + busy_timeout，读已提交批次不会被写入阻塞。

## 运行测试

```bash
pip install -r requirements.txt pytest
python -m pytest -q
```

40 个用例覆盖：正常往返、末行无换行、顺序与分页、404、各类 422（损坏/截断/
CRC/尾随数据/多成员/空行/非法 JSON/字段/重复 id/非 UTF-8/CRLF）、413
（2 MiB/8 MiB/64 KiB 及精确边界）、流式分块、提交前不可见、中断不留批次、
慢上传不阻塞其他上传与读取，以及极端分块（逐字节）的大批次完整性回归。
