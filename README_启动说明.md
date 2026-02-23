# 自动启动说明

## 指定节点数量的三种方式

### 方式1：命令行参数（推荐）

**Windows批处理：**
```bash
start_all.bat 6    # 启动6个节点（节点0-5）
```

**PowerShell：**
```powershell
.\start_all.ps1 -NodeCount 6    # 启动6个节点（节点0-5）
```

**Linux/Mac：**
```bash
./start_all.sh 6    # 启动6个节点（节点0-5）
```

### 方式2：配置文件

1. 复制 `nodes.conf.example` 为 `nodes.conf`
2. 在 `nodes.conf` 第一行写入节点数量，例如：
   ```
   6
   ```
3. 运行脚本（不需要参数）：
   ```bash
   start_all.bat
   ```

### 方式3：使用默认值

如果不指定节点数量且没有配置文件，默认启动4个节点。

## Windows系统

### 方法1：使用批处理脚本（推荐）

**启动4个节点（默认）：**
```bash
start_all.bat
```

**启动指定数量的节点：**
```bash
start_all.bat 6    # 启动6个节点
```

### 方法2：使用PowerShell脚本

1. 右键点击 `start_all.ps1`
2. 选择"使用PowerShell运行"
3. 如果遇到执行策略限制，先运行：
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

**启动指定数量的节点：**
```powershell
.\start_all.ps1 -NodeCount 6
```

## Linux/Mac系统

1. 给脚本添加执行权限：
   ```bash
   chmod +x start_all.sh
   ```

2. 运行脚本：
   ```bash
   ./start_all.sh        # 使用默认值或配置文件
   ./start_all.sh 6      # 启动6个节点
   ```

## 手动启动（如果需要）

如果自动启动脚本不工作，可以手动在5个终端中分别运行：

**终端1：**
```bash
程序名 0
```

**终端2：**
```bash
程序名 1
```

**终端3：**
```bash
程序名 2
```

**终端4：**
```bash
程序名 3
```

**终端5：**
```bash
程序名 client
```

## 注意事项

1. 确保可执行文件名为 `bft.exe`（Windows）或 `bft`（Linux/Mac）
2. 如果可执行文件名不同，请修改脚本中的 `EXE_NAME` 变量
3. 脚本会在每个节点启动之间等待1秒，确保端口绑定成功
4. 客户端会在所有服务端启动3秒后才启动，确保服务端已就绪
5. **节点数量必须与代码中的 `nodeCount` 常量一致**，否则会导致共识失败
6. 如果修改了 `main.go` 中的 `nodeCount`，记得同步修改启动脚本的参数或配置文件

