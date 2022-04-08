REM 2A test 
@echo off
for /l %%a in (1,1,10) do (
  go test -run 2A -race 
)

REM 2B test 
@echo off
for /l %%a in (1,1,10) do (
  go test -run 2B -race 
)

REM 2C test 
@echo off
for /l %%a in (1,1,10) do (
  go test -run 2C -race 
)

REM 2D test 
@echo off
for /l %%a in (1,1,10) do (
  go test -run 2D -race 
)
pause
