pushd %~dp0

set MICROSOFT_NET_COMPILERS_VERSION=1.1.1

sparkclr-submit.cmd --conf spark.local.dir=..\Temp --name SparkCLRShell %* --exe csi.exe ..\shell /r:..\shell\Microsoft.Spark.CSharp.Adapter.dll,..\shell\Newtonsoft.Json.dll,..\shell\Razorvine.Pyrolite.dll,..\shell\Razorvine.Serpent.dll,..\shell\log4net.dll  /i init.csx

popd