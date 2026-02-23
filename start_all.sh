#!/bin/bash
# Linux/Mac脚本：自动启动所有服务端节点和客户端
# 使用方法: ./start_all.sh [节点数量]
# 例如: ./start_all.sh 6  (启动6个节点: 0-5)

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXE_NAME="./bft"

# 检查可执行文件是否存在
if [ ! -f "$SCRIPT_DIR/$EXE_NAME" ]; then
    echo "错误: 找不到 $EXE_NAME"
    echo "请确保可执行文件在当前目录下"
    exit 1
fi

# 获取节点数量参数
NODE_COUNT=$1

# 如果没有提供参数，尝试从配置文件读取
if [ -z "$NODE_COUNT" ]; then
    if [ -f "$SCRIPT_DIR/nodes.conf" ]; then
        NODE_COUNT=$(head -n 1 "$SCRIPT_DIR/nodes.conf")
        echo "从配置文件读取节点数量: $NODE_COUNT"
    else
        # 默认使用4个节点
        NODE_COUNT=4
        echo "未指定节点数量，使用默认值: $NODE_COUNT"
        echo "提示: 可以通过以下方式指定节点数量："
        echo "  1. 命令行参数: ./start_all.sh 6"
        echo "  2. 配置文件: 创建 nodes.conf 文件，第一行写入节点数量"
        echo ""
    fi
fi

# 验证节点数量是否为有效数字
if ! [[ "$NODE_COUNT" =~ ^[0-9]+$ ]] || [ "$NODE_COUNT" -le 0 ]; then
    echo "错误: 节点数量必须是正整数"
    exit 1
fi

MAX_NODE=$((NODE_COUNT - 1))

echo "========================================"
echo "自动启动所有服务端节点和客户端"
echo "========================================"
echo ""
echo "配置信息:"
echo "  节点数量: $NODE_COUNT"
echo "  节点ID范围: 0-$MAX_NODE"
echo ""

echo "正在启动服务端节点..."
echo ""

# 循环启动所有节点
for ((i=0; i<NODE_COUNT; i++)); do
    echo "启动节点$i..."
    
    # 尝试不同的终端程序
    if command -v gnome-terminal &> /dev/null; then
        gnome-terminal --title="节点$i - 服务端" -- bash -c "cd '$SCRIPT_DIR' && $EXE_NAME $i; exec bash" 2>/dev/null
    elif command -v xterm &> /dev/null; then
        xterm -title "节点$i - 服务端" -e "cd '$SCRIPT_DIR' && $EXE_NAME $i; exec bash" 2>/dev/null &
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        osascript -e "tell app \"Terminal\" to do script \"cd '$SCRIPT_DIR' && $EXE_NAME $i\"" 2>/dev/null
    else
        echo "错误: 找不到可用的终端程序"
        exit 1
    fi
    
    sleep 1
done

echo ""
echo "等待服务端节点启动完成..."
sleep 3

echo ""
echo "启动客户端..."

# 启动客户端
if command -v gnome-terminal &> /dev/null; then
    gnome-terminal --title="客户端" -- bash -c "cd '$SCRIPT_DIR' && $EXE_NAME client; exec bash" 2>/dev/null
elif command -v xterm &> /dev/null; then
    xterm -title "客户端" -e "cd '$SCRIPT_DIR' && $EXE_NAME client; exec bash" 2>/dev/null &
elif [[ "$OSTYPE" == "darwin"* ]]; then
    osascript -e "tell app \"Terminal\" to do script \"cd '$SCRIPT_DIR' && $EXE_NAME client\"" 2>/dev/null
fi

echo ""
echo "========================================"
echo "所有节点和客户端已启动！"
echo "========================================"
echo "已打开以下窗口："
for ((i=0; i<NODE_COUNT; i++)); do
    echo "  - 节点$i - 服务端"
done
echo "  - 客户端"
echo ""
echo "总共启动了 $NODE_COUNT 个服务端节点和 1 个客户端"
echo ""
