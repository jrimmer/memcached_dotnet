using System;
using System.Runtime.Serialization;

namespace red5.memcached.Exceptions
{
    /// <summary>
    /// Socket exception
    /// </summary>
    public class SocketException : MemcachedException
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public SocketException() : base("Socket error")
        {
        }

        /// <summary>
        /// Constructor that accepts a message
        /// </summary>
        /// <param name="message">Description of exception</param>
        public SocketException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor that accepts a message
        /// </summary>
        /// <param name="inner">Inner exception</param>
        public SocketException(Exception inner) : base(inner)
        {
        }

        /// <summary>
        /// Constructor that accepts and wraps an inner exception
        /// </summary>
        /// <param name="message">Description of exception</param>
        /// <param name="inner">Wrapped exception</param>
        public SocketException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// DeSerialization Constructor
        /// </summary>
        /// <param name="info">Serialization info</param>
        /// <param name="ctx">Streaming context</param>
        protected SocketException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
        }
    }
}


