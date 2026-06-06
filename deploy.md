# Stellflow Service systemd 部署说明

本文档说明如何将已经上传到服务器指定目录的 `stellflow-service` 可执行 jar 包加入 `systemctl` 管控。

## 1. Problem analysis

`stellflow-service` 通过 Maven 打包后会生成可执行 jar。服务器上只需要将 jar 放到固定目录，并创建一个 systemd service unit，即可使用 `systemctl start|stop|restart|status` 管理进程。

本文档假设服务器上的部署目录为：

```bash
/data/stellflow-service
```

建议目录结构如下：

```bash
/data/stellflow-service
├── stellflow-service-0.0.1.jar
├── stellflow.yaml
├── data
└── logs
```

其中：

- `stellflow-service-0.0.1.jar`：已上传的可执行 jar。
- `stellflow.yaml`：服务配置文件，可从项目 `src/main/resources/stellflow.yaml` 复制后按环境修改。
- `data`：运行数据目录。
- `logs`：日志目录。

## 2. Design

systemd unit 只负责定义服务的启动方式、工作目录、重启策略和 JVM 参数。

推荐使用：

- `WorkingDirectory=/data/stellflow-service`
- `ExecStart=<java绝对路径> ... -jar /data/stellflow-service/stellflow-service-0.0.1.jar`
- `Restart=on-failure`
- `WantedBy=multi-user.target`

如果服务使用外部配置文件，启动命令中需要显式指定：

```bash
-Dstellflow.config.file=/data/stellflow-service/stellflow.yaml
```

## 3. Implementation

### 3.1 创建部署目录

```bash
sudo mkdir -p /data/stellflow-service/data
sudo mkdir -p /data/stellflow-service/logs
```

将 jar 包上传到：

```bash
/data/stellflow-service/stellflow-service-0.0.1.jar
```

将配置文件上传到：

```bash
/data/stellflow-service/stellflow.yaml
```

确认 Java 可执行文件路径：

```bash
which java
readlink -f "$(which java)"
java -version
```

如果输出不是 `/usr/bin/java`，需要把下面 systemd unit 中的 `ExecStart` 第一个路径替换为真实 Java 路径。

### 3.2 创建 systemd service 文件

创建文件：

```bash
sudo vi /etc/systemd/system/stellflow-service.service
```

写入以下内容：

```ini
[Unit]
Description=Stellflow Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/data/stellflow-service
ExecStart=/usr/bin/java -Xms512m -Xmx512m -Dstellflow.config.file=/data/stellflow-service/stellflow.yaml -Dstellflow.storage.log.rootDir=/data/stellflow-service/data/logs -jar /data/stellflow-service/stellflow-service-0.0.1.jar
Restart=on-failure
RestartSec=5
SuccessExitStatus=143
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

如果 jar 包名称不同，只需要修改 `ExecStart` 中 `-jar` 后面的路径。

注意：`ExecStart` 建议保持为单行。systemd 不通过 shell 执行该命令，复制多行命令时如果换行、反斜杠或缩进不符合 systemd unit 语法，可能导致 `status=203/EXEC`。

### 3.3 重新加载 systemd

```bash
sudo systemctl daemon-reload
```

### 3.4 设置开机自启

```bash
sudo systemctl enable stellflow-service
```

### 3.5 启动服务

```bash
sudo systemctl start stellflow-service
```

### 3.6 查看服务状态

```bash
sudo systemctl status stellflow-service
```

### 3.7 查看运行日志

```bash
sudo journalctl -u stellflow-service -f
```

### 3.8 重启服务

```bash
sudo systemctl restart stellflow-service
```

### 3.9 停止服务

```bash
sudo systemctl stop stellflow-service
```

### 3.10 排查 status=203/EXEC

`status=203/EXEC` 表示 systemd 无法执行 `ExecStart` 指定的可执行文件。这个错误通常发生在 Java 进程启动之前。

优先检查以下几项：

```bash
which java
readlink -f "$(which java)"
ls -l /usr/bin/java
ls -l /data/stellflow-service/stellflow-service-0.0.1.jar
sudo systemd-analyze verify /etc/systemd/system/stellflow-service.service
sudo systemctl cat stellflow-service
```

常见原因和处理方式：

- `/usr/bin/java` 不存在：把 `ExecStart` 的第一个路径改为 `readlink -f "$(which java)"` 输出的真实路径。
- `ExecStart` 被写成多行后格式不正确：改为本文档中的单行 `ExecStart`。
- jar 路径写错：确认 `/data/stellflow-service/stellflow-service-0.0.1.jar` 文件存在。
- `/data` 挂载了 `noexec`：jar 本身不需要执行权限，但如果 `ExecStart` 指向了 `/data` 下的脚本，脚本会受 `noexec` 影响。本文档推荐直接执行 Java，避免这个问题。

修改 unit 后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl reset-failed stellflow-service
sudo systemctl restart stellflow-service
sudo systemctl status stellflow-service
```

## 4. Complete code

完整的 systemd unit 内容如下：

```ini
[Unit]
Description=Stellflow Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/data/stellflow-service
ExecStart=/usr/bin/java -Xms512m -Xmx512m -Dstellflow.config.file=/data/stellflow-service/stellflow.yaml -Dstellflow.storage.log.rootDir=/data/stellflow-service/data/logs -jar /data/stellflow-service/stellflow-service-0.0.1.jar
Restart=on-failure
RestartSec=5
SuccessExitStatus=143
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```
