@echo off
chcp 65001 >nul
echo ========================================
echo 关闭占用指定端口的进程
echo ========================================
echo.

REM 检查是否提供了端口参数
set PORT=%1
if "%PORT%"=="" (
    echo 用法: kill_port.bat <端口号>
    echo 示例: kill_port.bat 8888
    echo.
    pause
    exit /b 1
)

echo 正在检查端口 %PORT% 的占用情况...
echo.

REM 查找占用端口的进程
set FOUND=0
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%PORT%') do (
    if not "%%a"=="" (
        set FOUND=1
        echo 发现进程 PID: %%a
        echo 正在关闭进程...
        taskkill /PID %%a /F >nul 2>&1
        if not errorlevel 1 (
            echo   ✓ 已关闭占用端口%PORT%的进程 (PID: %%a)
        ) else (
            echo   ✗ 关闭进程失败 (PID: %%a)，可能需要管理员权限
        )
    )
)

if %FOUND%==0 (
    echo 端口 %PORT% 未被占用
)

echo.
echo ========================================
echo 操作完成
echo ========================================
echo.
pause

