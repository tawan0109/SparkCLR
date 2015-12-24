#r ..\shell\Microsoft.Spark.CSharp.Adapter.dll
#r ..\shell\Newtonsoft.Json.dll
#r ..\shell\Razorvine.Pyrolite.dll
#r ..\shell\Razorvine.Serpent.dll
#r ..\shell\log4net.dll

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

Console.Title = "SparkCLR Shell";

var sparkConf = new SparkConf();
sparkConf.SetAppName("SparkCLR Shell");

var sc = new SparkContext(sparkConf);
Console.WriteLine("Spark context available as sc.");

var sqlContext = new SqlContext(sc);
Console.WriteLine("SQL context available as sqlContext.");
Console.WriteLine();