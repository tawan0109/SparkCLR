pushd %~dp0

set MICROSOFT_NET_COMPILERS_VERSION=1.1.1

sparkclr-submit.cmd --conf spark.local.dir=%temp% --name SparkCLR_REPL %* --exe Repl.exe %SPARKCLR_HOME%\repl

popd