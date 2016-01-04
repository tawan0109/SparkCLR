using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
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
        public SqlContext SqlContext;
    }

    internal class CSharpScriptEngine
    {
        private static ScriptState<object> previousState;
        private static int seq = 0;
        private static string dllDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        private static SparkCLRHost host;

        public static object Execute(string code)
        {
            Script<object> script;
            if (previousState == null)
            {
                script = CSharpScript.Create(code, globalsType:typeof(SparkCLRHost)).WithOptions(
                    ScriptOptions.Default
                    .AddReferences("System")
                    .AddReferences("System.Core")
                    .AddReferences("Microsoft.CSharp")
                    .AddReferences(typeof(SparkContext).Assembly)
                    .AddReferences(typeof(Pickler).Assembly)
                    .AddReferences(typeof(Parser).Assembly));

                Environment.SetEnvironmentVariable("SPARKCLR_RUN_MODE", "shell");
                Environment.SetEnvironmentVariable("SPARKCLR_SHELL_DLL_DIR", dllDirectory);
            }
            else
            {
                script = previousState.Script.ContinueWith(code);
            }

            // diagnostic
            // refer to http://source.roslyn.io/#Microsoft.CodeAnalysis.Scripting/Hosting/CommandLine/CommandLineRunner.cs,268

            /*var diagnostics = script.Compile();
            foreach (var diagnostic in diagnostics)
            {
                if (diagnostic.Severity == DiagnosticSeverity.Error)
                {
                    return "Compile error.";
                }
            }*/
            PersistCompilation(script.GetCompilation());
            if (host == null)
            {
                SparkConf conf = new SparkConf();
                SparkContext sc = new SparkContext(conf);
                host = new SparkCLRHost();
                host.sc = sc;
            }
            ScriptState<object> endState = null;
            if (previousState == null)
            {
                endState = script.RunAsync(host).Result;
            }
            else
            {
                // https://github.com/dotnet/roslyn/issues/6612
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

        private static void PersistCompilation(Compilation compilation)
        {
            //var compilation = script.GetCompilation();
            using (FileStream stream = new FileStream(string.Format(@"{0}\{1}.dll", dllDirectory, seq++), FileMode.CreateNew))
            {
                compilation.Emit(stream);
            }
        }
    }

    class Shell
    {
        static void Main(string[] args)
        {
            CSharpScriptEngine.Execute(@"
using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
");

            while (true)
            {
                Console.Write("> ");
                string line = Console.ReadLine();
                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }
                if (line.Trim().Equals(":quit", StringComparison.InvariantCultureIgnoreCase))
                {
                    break;
                }
                Console.WriteLine(CSharpScriptEngine.Execute(line));
            }
        }
    }
}
