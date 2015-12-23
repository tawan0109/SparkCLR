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
Console.WriteLine("Use quit() to exit.");
Console.WriteLine();

public static class REPLOperation
{
        public static void Quit()
        {
            System.Environment.Exit(0);
        }
}

Action quit = REPLOperation.Quit;