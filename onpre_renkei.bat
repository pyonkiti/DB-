@echo off
rem タスクスケジューラに登録して、５秒毎に「onpre_renkei.rb」を実行します
rem Rubyの引数は顧客毎に設定します
cd C:\Ruby24-x64\bin
set /a COUNT=0

:loop
powershell sleep 5
set /a COUNT=COUNT+1
echo (%COUNT%)
ruby C:\Users\NSK\onpre_renkei.rb 0
goto :loop