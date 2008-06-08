using System;
using System.Runtime.Serialization;

namespace red5.memcached.Exceptions
{
    /// <summary>
    /// Item not stored exception
    /// </summary>
    public class NotStoredException : MemcachedException
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public NotStoredException() : base("Item not stored")
        {
        }

        /// <summary>
        /// Constructor that accepts a message
        /// </summary>
        /// <param name="message">Description of exception</param>
        public NotStoredException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor that accepts a message
        /// </summary>
        /// <param name="inner">Inner exception</param>
        public NotStoredException(Exception inner) : base(inner)
        {
        }

        /// <summary>
        /// Constructor that accepts and wraps an inner exception
        /// </summary>
        /// <param name="message">Description of exception</param>
        /// <param name="inner">Wrapped exception</param>
        public NotStoredException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// DeSerialization Constructor
        /// </summary>
        /// <param name="info">Serialization info</param>
        /// <param name="ctx">Streaming context</param>
        protected NotStoredException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
        }
    }
}


