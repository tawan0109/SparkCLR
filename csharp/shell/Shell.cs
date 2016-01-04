using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp
{

    public class SparkCLRHost
    {
        public SparkContext sc;
    }

    internal class CSharpScriptEngine
    {
        private static ScriptState<object> _previousState;

        private static int counter = 0;

        private static string dllDirectory = @"d:\temp\dll";

        private static SparkCLRHost host;

        public static object Execute(string code)
        {
            Script<object> script;
            if (_previousState == null)
            {
                script = CSharpScript.Create(code, globalsType:typeof(SparkCLRHost)).WithOptions(
                    ScriptOptions.Default
                    .AddReferences("System")
                    .AddReferences("System.Core")
                    .AddReferences("Microsoft.CSharp")
                    .AddReferences(typeof(SparkContext).Assembly)
                    );

                Environment.SetEnvironmentVariable("SPARKCLR_RUN_MODE", "shell");
                Environment.SetEnvironmentVariable("SPARKCLR_SHELL_DLL_DIR", dllDirectory);
            }
            else
            {
                script = _previousState.Script.ContinueWith(code);
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
            if (_previousState == null)
            {
                endState = script.RunAsync(host).Result;
            }
            else
            {
                // https://github.com/dotnet/roslyn/issues/6612
                var m = script.GetType().GetMethod("ContinueAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                if (m != null)
                {
                    endState = ((Task<ScriptState<object>>)m.Invoke(script, new object[] { _previousState, default(CancellationToken) })).Result;
                }
                else
                {
                    throw new InvalidOperationException("Can't find method ContinueAsync!!!");
                }
            }
            _previousState = endState;
            return endState.ReturnValue;
        }

        private static void PersistCompilation(Compilation compilation)
        {
            //var compilation = script.GetCompilation();
            using (FileStream stream = new FileStream(string.Format(@"{0}\{1}.dll", dllDirectory, counter++), FileMode.CreateNew))
            {
                compilation.Emit(stream);
            }
            Console.WriteLine("Emitted " + (counter-1) + ".dll");
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
                if (line == null)
                {
                    continue;
                }
                if (line.Equals("quit", StringComparison.InvariantCultureIgnoreCase))
                {
                    break;
                }
                Console.WriteLine(CSharpScriptEngine.Execute(line));
            }
        }
    }
}
