// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Samples;
using Microsoft.Spark.CSharp.Streaming;
using StreamingContext = Microsoft.Spark.CSharp.Streaming.StreamingContext;

namespace Microsoft.Spark.CSharp
{
    internal class DStreamSamples2
    {
        private static int count;
        private static bool stopFileServer;
        private static void StartFileServer(string directory, string pattern, int loop)
        {
            string testDir = Path.Combine(directory, "test");
            if (!Directory.Exists(testDir))
                Directory.CreateDirectory(testDir);

            stopFileServer = false;

            string[] files = Directory.GetFiles(directory, pattern);

            Task.Run(() =>
            {
                while (!stopFileServer)
                {
                    DateTime now = DateTime.Now;
                    foreach (string path in files)
                    {
                        string text = File.ReadAllText(path);
                        File.WriteAllText(testDir + "\\" + now.ToBinary() + "_" + Path.GetFileName(path), text);
                    }
                    System.Threading.Thread.Sleep(200);
                }

                System.Threading.Thread.Sleep(3000);

                foreach (var file in Directory.GetFiles(testDir, "*"))
                    File.Delete(file);
            });
        }


        [Serializable]
        internal class SerializeHelper<M>
        {
            [NonSerialized]
            private IFormatter formatter = new BinaryFormatter();

            internal byte[] Execute(M v)
            {
                if (formatter == null)
                {
                    formatter = new BinaryFormatter();
                }

                var ms = new MemoryStream();
                formatter.Serialize(ms, v);
                return ms.ToArray();
            }
        }

        [Sample("experimental")]
        internal static void DStreamMapWithStateSamples()
        {
            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    SparkContext sc = SparkCLRSamples.SparkContext;
                    StreamingContext context = new StreamingContext(sc, 30000);
                    // context.Checkpoint(checkpointPath);

                    var lines = context.TextFileStream(Path.Combine(directory, "test"));
                    var pairs = lines.Map(w => new KeyValuePair<string, int>(w, 1));
                    // var state = lines.Map(new SerializeHelper<string>().Execute);
                    /*
                    var state = pairs.Map(kv => BitConverter.GetBytes(1));
                     */

                    var state = pairs.MapWithState<string, int, int, int>(
                        (batchTime, word, v, s) =>
                        {
                            var total = 0;
                            try
                            {
                                total = s.Get();
                            }
                            catch (Exception)
                            { 
                                // todo
                            }
                            total += v;
                            s.Update(total);
                            return total;
                        });
                    
                   // var state = join.UpdateStateByKey<string, Tuple<int, int>, int>((vs, s) => vs.Sum(x => x.Item1 + x.Item2) + s);
                    state.ForeachRDD((time, rdd) =>
                    {
                        // there's chance rdd.Take conflicts with ssc.Stop
                        if (stopFileServer)
                            return;

                        Console.WriteLine(rdd.Info());

                        object[] taken = rdd.Take(10);
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");
                        foreach (dynamic record in taken)
                        {
                            // Console.WriteLine(record);
                            Console.WriteLine(BitConverter.ToString(record).Replace("-", string.Empty));
                        }
                        Console.WriteLine();

                        stopFileServer = count++ > 100;
                    });
                    /*
                    state.ForeachRDD((time, rdd) =>
                    {
                        // there's chance rdd.Take conflicts with ssc.Stop
                        if (stopFileServer)
                            return;

                        object[] taken = rdd.Take(10);
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");
                        foreach (object record in taken)
                        {
                            Console.WriteLine(record);
                        }
                        Console.WriteLine();

                        stopFileServer = count++ > 100;
                    });
                    */

                    return context;
                });

            ssc.Start();

            StartFileServer(directory, "words.txt", 100);

            ssc.AwaitTermination();
            ssc.Stop();
        }
    }
}
