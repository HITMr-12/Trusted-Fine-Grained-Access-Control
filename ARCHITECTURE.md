# 组件与信任边界

```text
Spark / PostgreSQL
    |  独立热加载插件
    |-- 普通对象 --> Polaris 分配 --> storage/data/public
    `-- 受控子树 --> Remote ------------------------------+
                         |                                |
                         | 授权、策略、对象版本验证       |
                         v                                |
                  FGAC Polaris                           |
                         | Remote 专用存储授权            |
                         v                                |
                  storage/data/governed                  |
                         | 行过滤、掩码、获批算子         |
                         +---------- 治理后数据 ----------+
```

## 明确边界

- Polaris 是控制面：对象、身份、RBAC、FGAC 策略、执行路由和授权契约。
- Remote 是独立数据面：读取受控对象并实际执行行过滤、掩码和安全算子。
- 插件是独立引擎适配层：截获原生计划、阻断本地受控扫描、提交子计划并接回结果。
- 普通引擎不能获得受控目录或原始存储凭据。
- Polaris 不执行数据扫描，也不依赖 Spark/PG 插件制品。

## Polaris 待实现接口

为保持 Remote 与插件稳定，第一阶段在 Polaris 中实现兼容扩展：

```text
GET  /v2/relations/{namespace}/{table}
POST /v2/authorize
```

随后再将内部实现对接 Polaris Table/GenericTable、Principal/Role、Policy Store 和
credential vending，并逐步演进为带版本的 Polaris FGAC API。
