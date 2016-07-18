﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;
using Razorvine.Serpent;

namespace Microsoft.Spark.CSharp
{
    public class SparkCLRHost
    {
        public SparkContext sc;
        public SqlContext sqlContext;
    }

    internal class CSharpScriptEngine
    {
        private static ScriptState<object> previousState;
        private static int seq = 0;
        private static string dllDumpDirectory;

        private static SparkConf sparkConf;
        private static SparkCLRHost host;

        static CSharpScriptEngine()
        {
            sparkConf = new SparkConf();
            var sparkLocalDir = sparkConf.Get("spark.local.dir", Path.GetTempPath());
            dllDumpDirectory = Path.Combine(sparkLocalDir, Path.GetRandomFileName());
            Directory.CreateDirectory(dllDumpDirectory);
        }

        internal static bool IsCompleteSubmission(string code)
        {
            var options = new CSharpParseOptions(LanguageVersion.CSharp6, DocumentationMode.Diagnose, SourceCodeKind.Script);
            SyntaxTree syntaxTree = SyntaxFactory.ParseSyntaxTree(code, options);
            return SyntaxFactory.IsCompleteSubmission(syntaxTree);
        }

        internal static object Execute(string code)
        {
            Script<object> script;
            if (previousState == null)
            {
                script = CSharpScript.Create(code, globalsType: typeof(SparkCLRHost)).WithOptions(
                    ScriptOptions.Default
                    .AddReferences("System")
                    .AddReferences("System.Core")
                    .AddReferences("Microsoft.CSharp")
                    .AddReferences(typeof(SparkContext).Assembly)
                    .AddReferences(typeof(Pickler).Assembly)
                    .AddReferences(typeof(Parser).Assembly));

                Environment.SetEnvironmentVariable("SPARKCLR_RUN_MODE", "shell");
                Environment.SetEnvironmentVariable("SPARKCLR_SCRIPT_COMPILATION_DIR", dllDumpDirectory);
            }
            else
            {
                script = previousState.Script.ContinueWith(code);
            }

            var diagnostics = script.Compile();
            bool hasErrors = Enumerable.Any(diagnostics, diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);

            if (hasErrors)
            {
                DisplayErrors(script.Compile());
                return null;
            }

            PersistCompilation(script.GetCompilation());
            if (host == null)
            {
                var conf = new SparkConf();
                var sc = new SparkContext(conf);
                host = new SparkCLRHost
                {
                    sc = sc,
                    sqlContext = new SqlContext(sc)
                };
            }
            ScriptState<object> endState = null;
            if (previousState == null)
            {
                endState = script.RunAsync(host).Result;
            }
            else
            {
                // "ContinueAsync" is a internal methold now, might be public in 1.2.0(https://github.com/dotnet/roslyn/issues/6612)
                const string methodName = "ContinueAsync";
                var m = script.GetType().GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                if (m != null)
                {
                    endState = ((Task<ScriptState<object>>)m.Invoke(script, new object[] { previousState, default(CancellationToken) })).Result;
                }
                else
                {
                    throw new InvalidOperationException(string.Format("Can't find method {0}", methodName));
                }
            }
            previousState = endState;
            return endState.ReturnValue;
        }

        private static void DisplayErrors(IEnumerable<Diagnostic> diagnostics)
        {
            // refer to http://source.roslyn.io/#Microsoft.CodeAnalysis.Scripting/Hosting/CommandLine/CommandLineRunner.cs,268
            try
            {
                foreach (var diagnostic in diagnostics)
                {
                    Console.ForegroundColor = (diagnostic.Severity == DiagnosticSeverity.Error) ? ConsoleColor.Red : ConsoleColor.Yellow;
                    Console.WriteLine(diagnostic.ToString());
                }
            }
            finally
            {
                Console.ResetColor();
            }
        }

        private static void PersistCompilation(Compilation compilation)
        {
            using (FileStream stream = new FileStream(string.Format(@"{0}\{1}.dll", dllDumpDirectory, seq++), FileMode.CreateNew))
            {
                compilation.Emit(stream);
            }
        }

        public static void DeleteTempFiles()
        {
            // delete DLLs dump directory on exit
            if (Directory.Exists(dllDumpDirectory))
            {
                Directory.Delete(dllDumpDirectory, true);
            }
        }
    }

    class Repl
    {
        static void Main(string[] args)
        {
            CSharpScriptEngine.Execute(@"
            using System;
            using System.Collections.Generic;
            using Microsoft.Spark.CSharp.Core;
            using Microsoft.Spark.CSharp.Interop;
            ");
            Console.WriteLine("Spark context available as sc.");
            Console.WriteLine("SQL context available as sqlContext.");
            Console.WriteLine("Use :quit to exit.");

            while (true)
            {
                Console.Write("> ");
                var inputLines = new StringBuilder();
                bool cancelSubmission = false;
 
                while (true)
                {
                    var line = Console.ReadLine();

                    if (string.IsNullOrEmpty(line))
                    {
                        cancelSubmission = true;
                        break;
                    }

                    if (line.Trim().Equals(":quit", StringComparison.InvariantCultureIgnoreCase))
                    {
                        CSharpScriptEngine.DeleteTempFiles();
                        return;
                    }    

                    inputLines.AppendLine(line);

                    if (CSharpScriptEngine.IsCompleteSubmission(inputLines.ToString()))
                    {
                        break;
                    }

                    Console.Write(". ");
                }

                if (cancelSubmission)
                {
                    continue;
                }

                var returnValue = CSharpScriptEngine.Execute(inputLines.ToString());
                if (returnValue != null)
                {
                    Console.WriteLine(returnValue);
                }
            }
        }
    }
}
