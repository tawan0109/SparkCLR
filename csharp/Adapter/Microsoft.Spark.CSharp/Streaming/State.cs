using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Streaming
{
    [Serializable]
    public class State<S>
    {
        private S state = default(S);

        private bool defined = false;
        private bool timingOut = false;
        private bool updated = false;
        private bool removed = false;

        public bool Exists()
        {
            return defined;
        }

        public S Get()
        {
            if (defined)
            {
                return state;
            }
            else
            {
                throw new ArgumentException("State is not set");
            }
        }

        public void Update(S newState)
        {
            if (removed || timingOut)
            {
                throw new ArgumentException("Cannot update the state that is timing out or has been removed.");
            }
            state = newState;
            defined = true;
            updated = true;
        }

        public void Remove()
        {
            if (removed || timingOut)
            {
                throw new ArgumentException("Cannot update the state that is timing out or has already been removed.");
            }
            defined = false;
            updated = false;
            removed = true;
        }

        public bool IsTimingOut()
        {
            return timingOut;
        }

        internal State<S> Wrap(S state)
        {
            this.state = state;
            if (object.ReferenceEquals(null, state))
            {
                defined = false;
            }
            else
            {
                defined = true;
            }

            timingOut = false;
            removed = false;
            updated = false;
            return this;
        }
    }
}
