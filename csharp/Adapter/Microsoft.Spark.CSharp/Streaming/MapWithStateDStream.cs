using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    /// <typeparam name="S"></typeparam>
    /// <typeparam name="M"></typeparam>
    public class MapWithStateDStream<K, V, S, M> : DStream<M>
    {
        internal DStream<KeyValuePair<K, V>> dataStream;

        internal StateSpec<K, V, S, M> spec;

        internal StreamingContext ssc;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ssc"></param>
        public MapWithStateDStream(StreamingContext ssc, DStream<KeyValuePair<K, V>> dataStream)
        {
            this.ssc = ssc;
            this.dataStream = dataStream;
        }

        public DStream<KeyValuePair<K, V>> StateSnapShots()
        {
            return null;
        }
    }

    internal class InternalMapWithStateDStream
    {
        
    }
}
