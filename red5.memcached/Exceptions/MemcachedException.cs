using System;
using System.Runtime.Serialization;

namespace red5.memcached.Exceptions
{
    /// <summary>
    /// Generic memcached.net exception
    /// </summary>
    public class MemcachedException : ApplicationException
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public MemcachedException() : base("MemcachedException")
        {
        }

        /// <summary>
        /// Constructor that accepts a message
        /// </summary>
        /// <param name="message">Description of exception</param>
        public MemcachedException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor that accepts a message
        /// </summary>
        /// <param name="inner">Inner exception</param>
        public MemcachedException(Exception inner) : base(inner.GetType().ToString(), inner)
        {
        }

        /// <summary>
        /// Constructor that accepts and wraps an inner exception
        /// </summary>
        /// <param name="message">Description of exception</param>
        /// <param name="inner">Wrapped exception</param>
        public MemcachedException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// DeSerialization Constructor
        /// </summary>
        /// <param name="info">Serialization info</param>
        /// <param name="ctx">Streaming context</param>
        protected MemcachedException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
        }
    }
}

