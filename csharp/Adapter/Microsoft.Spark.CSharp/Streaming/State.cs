using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Streaming
{
    public class State<S>
    {
        public bool Exists()
        {
            // TODO
            return false;
        }

        public S Get()
        {
            return default(S);
        }

        public void Update(S newState)
        {
        }

        public void Remove()
        {
            
        }

        public bool IsTimingOut()
        {
            return default(bool);
        }
    }
}
