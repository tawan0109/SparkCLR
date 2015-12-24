pushd %~dp0

sparkclr-submit.cmd --conf spark.local.dir=..\Temp --name SparkCLRShell %* --exe scriptcs.exe ..\shell init.csx -r

popd