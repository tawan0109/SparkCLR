using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Streaming
{
    public class StateSpec<K, V, S, M>
    {
        public StateSpec<K, V, S, M> InitialState(RDD<KeyValuePair<K, V>> rdd)
        {
            // TODO
            return null;
        }

        public StateSpec<K, V, S, M> NumPartitions(int numPartitions)
        {
            // TODO
            return null;
        }

        public StateSpec<K, V, S, M> Timeout(int idleDuration)
        {
            // TODO
            return null;
        }

        internal StateSpec<K, V, S, M> Function(Func<double, K, Option<V>, State<S>> mappingFunction)
        {
            throw new InvalidOperationException("Unimplemented.");
        }
    }
}
