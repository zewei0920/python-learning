@REM ofm auto monitor task for windows
@REM execute the cmd as below to reg: auto start with machine
@REM cmd: schtasks /create /tn “ofm-auto-monitor-task” /tr abspath-to-this-script\ofm-auto-monitor-task.bat /sc onstart

@echo off

@REM define temp file 
set TempFile=%TEMP%\sthUnique.tmp

@REM define tasks that need to monitor
set taskname1=pi-jdbc-history-data-server
set taskscriptpath1=D:\huarun-crp-test\dpp-tsdata-history\bin\pi-jdbc-history-data-server.bat
@REM add other task define here ...


@REM ===== define moniter task ====== 
set INTERVAL=60
:Again  
echo Now start to monitor tasks...
call:monitor %taskname1% %taskscriptpath1%
@REM  call other task here ...
timeout %INTERVAL%
goto Again


@REM ====== define the execute function ======= 
@REM arg 1: $~1,taskname;
@REM arg 2: $~2,taskscriptpath
:monitor
wmic process where caption="java.exe" get processid,caption,commandline /value | findstr %~1 >%TempFile%
if %ERRORLEVEL% == 0 (
echo The program %~1 is running ok...
) else (
start %~2
echo The program has been restarted: %~1
)
goto:eof

pause