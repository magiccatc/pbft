@echo off
chcp 65001 >nul
echo ========================================
echo 关闭所有服务端节点和客户端
echo ========================================
echo.

REM 获取当前脚本所在目录
set SCRIPT_DIR=%~dp0

REM 检查是否提供了节点数量参数
set NODE_COUNT=%1
if "%NODE_COUNT%"=="" (
    REM 如果没有提供参数，尝试从配置文件读取
    if exist "%SCRIPT_DIR%nodes.conf" (
        set /p NODE_COUNT=<"%SCRIPT_DIR%nodes.conf"
    ) else (
        REM 默认使用4个节点
        set NODE_COUNT=4
        echo 未指定节点数量，使用默认值: 4
        echo 提示: 可以通过以下方式指定节点数量：
        echo   1. 命令行参数: stop_all.bat 6
        echo   2. 配置文件: 创建 nodes.conf 文件，第一行写入节点数量
        echo.
    )
)

REM 验证节点数量是否为有效数字
echo %NODE_COUNT%| findstr /r "^[0-9][0-9]*$" >nul
if errorlevel 1 (
    echo 错误: 无效的节点数量 "%NODE_COUNT%"
    echo 节点数量必须是正整数
    pause
    exit /b 1
)

REM 计算节点数量（从0开始，所以是0到NODE_COUNT-1）
set /a MAX_NODE=%NODE_COUNT%-1

echo 配置信息:
echo   节点数量: %NODE_COUNT%
echo   节点ID范围: 0-%MAX_NODE%
echo.

echo 正在关闭服务端节点和客户端...
echo.

REM 方法1: 通过窗口标题关闭（精确匹配）
set CLOSED_COUNT=0
set NODE_ID=0
:stop_loop
if %NODE_ID% GTR %MAX_NODE% goto :stop_end

echo 关闭节点%NODE_ID%...
taskkill /FI "WINDOWTITLE eq 节点%NODE_ID% - 服务端*" /F >nul 2>&1
if not errorlevel 1 (
    echo   ✓ 节点%NODE_ID%已关闭
    set /a CLOSED_COUNT=%CLOSED_COUNT%+1
) else (
    echo   - 节点%NODE_ID%未运行
)

set /a NODE_ID=%NODE_ID%+1
goto :stop_loop

:stop_end

REM 关闭客户端窗口
echo 关闭客户端...
taskkill /FI "WINDOWTITLE eq 客户端*" /F >nul 2>&1
if not errorlevel 1 (
    echo   ✓ 客户端已关闭
    set /a CLOSED_COUNT=%CLOSED_COUNT%+1
) else (
    echo   - 客户端未运行
)

echo.
REM 备用方案: 关闭所有 bft.exe 进程（确保完全关闭）
echo 检查并关闭所有剩余的 bft.exe 进程...
taskkill /FI "IMAGENAME eq bft.exe" /F >nul 2>&1
if not errorlevel 1 (
    echo   ✓ 已关闭所有剩余的 bft.exe 进程
) else (
    echo   - 没有找到运行中的 bft.exe 进程
)

echo.
REM 关闭占用客户端端口(8888)的进程
echo 检查并关闭占用客户端端口(8888)的进程...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8888') do (
    if not "%%a"=="" (
        echo   发现进程 PID: %%a
        taskkill /PID %%a /F >nul 2>&1
        if not errorlevel 1 (
            echo   ✓ 已关闭占用端口8888的进程 (PID: %%a)
        )
    )
)

REM 关闭占用节点端口(8600)的进程
echo 检查并关闭占用节点端口(8600)的进程...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8600') do (
    if not "%%a"=="" (
        echo   发现进程 PID: %%a
        taskkill /PID %%a /F >nul 2>&1
        if not errorlevel 1 (
            echo   ✓ 已关闭占用端口8600的进程 (PID: %%a)
        )
    )
)

echo.
echo ========================================
echo 所有节点和客户端已关闭！
echo ========================================
echo.
echo 按任意键关闭此窗口...
pause >nul

