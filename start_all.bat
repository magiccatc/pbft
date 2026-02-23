@echo off
chcp 65001 >nul
echo ========================================
echo 自动启动所有服务端节点和客户端
echo ========================================
echo.

REM 获取当前脚本所在目录
set SCRIPT_DIR=%~dp0
set EXE_NAME=bft.exe

REM 检查可执行文件是否存在
if not exist "%SCRIPT_DIR%%EXE_NAME%" (
    echo 错误: 找不到 %EXE_NAME%
    echo 请确保可执行文件在当前目录下
    pause
    exit /b 1
)

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
        echo   1. 命令行参数: start_all.bat 6
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

echo 正在启动服务端节点...
echo.

REM 循环启动所有节点
set NODE_ID=0
:start_loop
if %NODE_ID% GTR %MAX_NODE% goto :end_loop

echo 启动节点%NODE_ID%...
start "节点%NODE_ID% - 服务端" cmd /k "cd /d %SCRIPT_DIR% && %EXE_NAME% %NODE_ID%"
timeout /t 1 /nobreak >nul

set /a NODE_ID=%NODE_ID%+1
goto :start_loop

:end_loop

echo.
echo 等待服务端节点启动完成...
timeout /t 3 /nobreak >nul

echo.
echo 启动客户端...
start "客户端" cmd /k "cd /d %SCRIPT_DIR% && %EXE_NAME% client"

echo.
echo ========================================
echo 所有节点和客户端已启动！
echo ========================================
echo 已打开以下窗口：
set NODE_ID=0
:list_loop
if %NODE_ID% GTR %MAX_NODE% goto :list_end
echo   - 节点%NODE_ID% - 服务端
set /a NODE_ID=%NODE_ID%+1
goto :list_loop
:list_end
echo   - 客户端
echo.
echo 总共启动了 %NODE_COUNT% 个服务端节点和 1 个客户端
echo.
echo 按任意键关闭此窗口（不会关闭已启动的节点）...
pause >nul
