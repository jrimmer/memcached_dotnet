//
// MemCached CLR/.NET client
// 
// Copyright (c) 2004 Jason Rimmer <jrimmer@irth.net> and 
// Greg Whalin <greg@meetup.com>
// All rights reserved.
// 
// Ported from Greg Whalin's <greg@meetup.com> java client
// 
// See the memcached website:
// http://www.danga.com/memcached/
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later
// version.
// 
// This library is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE.  See the GNU Lesser General Public License for more
// details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
//

using System;
using System.Collections;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Text.RegularExpressions;

using log4net;

using ICSharpCode.SharpZipLib.GZip;

using red5.memcached.Exceptions;

namespace red5.memcached
{
	/// <summary> This is a .NET client for the memcached server available from
	/// <a href="http:/www.danga.com/memcached/">http://www.danga.com/memcached/</a>.
	/// <br/> 
	/// Supports setting, adding, replacing, deleting compressed/uncompressed and<br/>
	/// serialized (can be stored as string if object is native class) objects to memcached.<br/>
	/// <br/>
	/// Now pulls SockIO objects from SockIOPool, which is a connection pool.  The server failover<br/>
	/// has also been moved into the SockIOPool class.<br/>
	/// This pool needs to be initialized prior to the client working.  See docs from SockIOPool.<br/>
	/// <br/>
	/// Some examples of use follow.<br/>
	/// <h3>To create cache client object and set params:</h3>
	/// <pre> 
	/// MemCachedClient mc = new MemCachedClient();
	/// 
	/// // compression is enabled by default	
	/// mc.setCompressEnable(true);
	/// 
	/// // set compression threshhold to 4 KB (default: 15 KB)	
	/// mc.setCompressThreshold(4096);
	/// 
	/// // turn off serialization (defaults to on).
	/// // Should not do this in most cases.	
	/// mc.setSerialize(false);
	/// </pre>	
	/// <h3>To store an object:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "cacheKey1";	
	/// Object value = SomeClass.getObject();	
	/// mc.set(key, value);
	/// </pre> 
	/// <h3>To store an object using a custom server hashCode:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "cacheKey1";	
	/// Object value = SomeClass.getObject();	
	/// Integer hash = new Integer(45);	
	/// mc.set(key, value, hash);
	/// </pre> 
	/// The set method shown above will always set the object in the cache.<br/>
	/// The add and replace methods do the same, but with a slight difference.<br/>
	/// <ul>
	/// <li>add -- will store the object only if the server does not have an entry for this key</li>
	/// <li>replace -- will store the object only if the server already has an entry for this key</li>
	/// </ul> 
	/// <h3>To delete a cache entry:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "cacheKey1";	
	/// mc.delete(key);
	/// </pre> 
	/// <h3>To delete a cache entry using a custom hash code:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "cacheKey1";	
	/// Integer hash = new Integer(45);	
	/// mc.delete(key, hashCode);
	/// </pre> 
	/// <h3>To store a counter and then increment or decrement that counter:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "counterKey";	
	/// mc.storeCounter(key, new Integer(100));
	/// System.out.println("counter after adding      1: " mc.incr(key));	
	/// System.out.println("counter after adding      5: " mc.incr(key, 5));	
	/// System.out.println("counter after subtracting 4: " mc.decr(key, 4));	
	/// System.out.println("counter after subtracting 1: " mc.decr(key));	
	/// </pre> 
	/// <h3>To store a counter and then increment or decrement that counter with custom hash:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "counterKey";	
	/// Integer hash = new Integer(45);	
	/// mc.storeCounter(key, new Integer(100), hash);
	/// System.out.println("counter after adding      1: " mc.incr(key, 1, hash));	
	/// System.out.println("counter after adding      5: " mc.incr(key, 5, hash));	
	/// System.out.println("counter after subtracting 4: " mc.decr(key, 4, hash));	
	/// System.out.println("counter after subtracting 1: " mc.decr(key, 1, hash));	
	/// </pre> 
	/// <h3>To retrieve an object from the cache:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "key";	
	/// Object value = mc.get(key);	
	/// </pre> 
	/// <h3>To retrieve an object from the cache with custom hash:</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String key   = "key";	
	/// Integer hash = new Integer(45);	
	/// Object value = mc.get(key, hash);	
	/// </pre> 
	/// <h3>To retrieve an multiple objects from the cache</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String[] keys   = { "key", "key1", "key2" };
	/// Object value = mc.getMulti(keys);
	/// </pre> 
	/// <h3>To retrieve an multiple objects from the cache with custom hashing</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// String[] keys    = { "key", "key1", "key2" };
	/// Integer[] hashes = { new Integer(45), new Integer(32), new Integer(44) };
	/// Object value = mc.getMulti(keys, hashes);
	/// </pre> 
	/// <h3>To flush all items in server(s)</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// mc.flushAll();
	/// </pre> 
	/// <h3>To get stats from server(s)</h3>
	/// <pre>
	/// MemCachedClient mc = new MemCachedClient();
	/// Map stats = mc.stats();
	/// </pre> 
	/// 
	/// </summary>
	public class MemCachedClient
	{
		private static readonly ILog log = LogManager.GetLogger(typeof(MemCachedClient));
		
		private enum SetCommandType
        {
            Add,
            Replace,
            Set
        }

        private enum IncrDecrCommandType
        {
            Increment,
            Decrement
        }

        /// <summary>
        /// Enable storing all data using serialization.
        /// 
        /// This defaults to on, and as a general rule, should not be changed by the client.<br/>
        /// serialization is probably what we want.  However, in some rare cases, it may<br/>
        /// make sense to store object as a string representation.<br/>
        /// <br/>
        /// If this flag is off, and the object we are storing is NOT a native type wrapper,<br/>
        /// we will serialize regardless.
        /// </summary>
        /// 
        /// <value><CODE>true</CODE> to enable serialization, <CODE>false</CODE> to disable serialization</value>
        virtual public bool Serialize
		{
            get
            {
                return this.serialize;
            }

            set
			{
				this.serialize = value;
			}
		}

        /// <summary>
        /// Enable storing compressed data, provided it meets the threshold requirements.
        /// 
        /// If enabled, data will be stored in compressed form if it is<br/>
        /// longer than the threshold length set with setCompressThreshold(int)<br/>
        /// <br/>
        /// The default is that compression is enabled.<br/>
        /// <br/>
        /// Even if compression is disabled, compressed data will be automatically<br/>
        /// decompressed.
        /// </summary>
        /// 
        /// <value><CODE>true</CODE> to enable compression, <CODE>false</CODE> to disable compression</value>
        virtual public bool CompressEnable
		{
            get
            {
                return this.compressEnable;
            }

            set
			{
				this.compressEnable = value;
			}
		}

		/// <summary> Sets the required length for data to be considered for compression.
		/// 
		/// If the length of the data to be stored is not equal or larger than this value, it will
		/// not be compressed.
		/// 
		/// This defaults to 15 KB.
		/// </summary>
		/// 
		/// <value>required length of data to consider compression</value>
		virtual public long CompressThreshold
		{
            get
            {
                return this.compressThreshold;
            }

            set
			{
				this.compressThreshold = value;
			}	
		}
		
		/// <summary> Creates a new instance of MemCachedClient.</summary>
		public MemCachedClient()
		{
			Init();
		}
		
		/// <summary> Initializes client object to defaults.
		/// 
		/// This enables compression and sets compression threshhold to 15 KB.
		/// </summary>
		private void Init()
		{
			this.serialize = true;
			this.compressEnable = true;
			this.compressThreshold = 15360;
		}
		
		/// <summary> Deletes an object from cache given cache key.
		/// 
		/// </summary>
		/// <param name="key">the key to be removed
		/// </param>
		/// <returns> <code>true</code>, if the data was deleted successfully
		/// </returns>
		public virtual bool Delete(string key)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return DeleteItem(key, DateTime.MinValue, sock);
        }
		
		/// <summary> Deletes an object from cache given cache key and expiration date. 
		/// 
		/// </summary>
		/// <param name="key">the key to be removed
		/// </param>
		/// <param name="expiry">when to expire the record.
		/// </param>
		/// <returns> <code>true</code>, if the data was deleted successfully
		/// </returns>
		public virtual bool Delete(string key, DateTime expiry)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return DeleteItem(key, expiry, sock);
        }

        /// <summary>
        /// Deletes an object from cache given cache key and expiration date. 
        /// </summary>
        /// 
        /// <param name="key">the key to be removed</param>
        /// <param name="hashCode">Hash code to use</param>
        /// 
        /// <returns> <code>true</code>, if the data was deleted successfully</returns>
        public virtual bool Delete(string key, int hashCode)
        {
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return DeleteItem(key, DateTime.MinValue, sock);
        }


        /// <summary>
        /// Deletes an object from cache given cache key, a delete time, and an optional hashcode.
		/// 
		/// The item is immediately made non retrievable.<br/>
		/// Keep in mind {@link #add(String, Object) add} and {@link #replace(String, Object) replace}<br/>
		/// will fail when used with the same key will fail, until the server reaches the<br/>
		/// specified time. However, {@link #set(String, Object) set} will succeed,<br/>
		/// and the new value will not be deleted.
		/// </summary>
		/// 
		/// <param name="key">the key to be removed</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <param name="expiry">when to expire the record.</param>
		/// 
		/// <returns><code>true</code>, if the data was deleted successfully</returns>
		public virtual bool Delete(string key, int hashCode, DateTime expiry)
		{
			// get SockIO obj from hash or from key
			SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return DeleteItem(key, expiry, sock);
        }

        /// <summary> Stores data on the server; only the key and the value are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Set(string key, object value)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Set, key, value, DateTime.MinValue, sock);
        }

        /// <summary> Stores data on the server; only the key and the value are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Set(string key, object value, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Set, key, value, DateTime.MinValue, sock);
        }
		
		/// <summary> Stores data on the server; the key, value, and an expiration time are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="expiry">when to expire the record
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Set(string key, object value, DateTime expiry)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Set, key, value, expiry, sock);
        }
		
		/// <summary> Stores data on the server; the key, value, and an expiration time are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="expiry">when to expire the record
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Set(string key, object value, DateTime expiry, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Set, key, value, expiry, sock);
        }

        /// <summary> Adds data to the server; only the key and the value are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Add(string key, object value)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Add, key, value, DateTime.MinValue, sock);
        }
		
		/// <summary> Adds data to the server; the key, value, and an optional hashcode are passed in.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Add(string key, object value, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Add, key, value, DateTime.MinValue, sock);
        }
		
		/// <summary> Adds data to the server; the key, value, and an expiration time are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="expiry">when to expire the record
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Add(string key, object value, DateTime expiry)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Add, key, value, expiry, sock);
        }
		
		/// <summary> Adds data to the server; the key, value, and an expiration time are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="expiry">when to expire the record
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Add(string key, object value, DateTime expiry, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Add, key, value, expiry, sock);
        }
		
		/// <summary> Updates data on the server; only the key and the value are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Replace(string key, object value)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Replace, key, value, DateTime.MinValue, sock);
        }
		
		/// <summary> Updates data on the server; only the key and the value and an optional hash are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Replace(string key, object value, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Replace, key, value, DateTime.MinValue, sock);
        }
		
		/// <summary> Updates data on the server; the key, value, and an expiration time are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="expiry">when to expire the record
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Replace(string key, object value, DateTime expiry)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Replace, key, value, expiry, sock);
        }
		
		/// <summary> Updates data on the server; the key, value, and an expiration time are specified.
		/// 
		/// </summary>
		/// <param name="key">key to store data under
		/// </param>
		/// <param name="value">value to store
		/// </param>
		/// <param name="expiry">when to expire the record
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true, if the data was successfully stored
		/// </returns>
		public virtual bool Replace(string key, object value, DateTime expiry, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Replace, key, value, expiry, sock);
        }
	
		
		/// <summary> Store a counter to memcached given a key
		/// 
		/// </summary>
		/// <param name="key">cache key
		/// </param>
		/// <param name="counter">number to store
		/// </param>
		/// <returns> true/false indicating success
		/// </returns>
		public virtual bool StoreCounter(string key, long counter)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Set, key, counter, DateTime.MinValue, sock);
        }
		
		/// <summary> Store a counter to memcached given a hashcode
		/// 
		/// </summary>
		/// <param name="key">cache key
		/// </param>
		/// <param name="counter">number to store
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> true/false indicating success
		/// </returns>
		public virtual bool StoreCounter(string key, long counter, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return SetItem(SetCommandType.Set, key, counter, DateTime.MinValue, sock);
		}
		
		/// <summary> Returns value in counter at given key as long. 
		/// 
		/// </summary>
		/// <param name="key">cache ket
		/// </param>
		/// <returns> counter value or -1 if not found
		/// </returns>
		public virtual long GetCounter(string key)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return (long)GetItem(key, sock);
        }
		
		/// <summary> Returns value in counter at given key as long. 
		/// 
		/// </summary>
		/// <param name="key">cache ket
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> counter value
		/// </returns>
		public virtual long GetCounter(string key, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return (long)GetItem(key, sock);
		}
		
		/// <summary> Increment the value at the specified key by 1, and then return it.
		/// 
		/// </summary>
		/// <param name="key">key where the data is stored
		/// </param>
		/// <returns> -1, if the key is not found, the value after incrementing otherwise
		/// </returns>
		public virtual long Increment(string key)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");
            
            return IncrDecrItem(IncrDecrCommandType.Increment, key, 1, sock);
        }
		
		/// <summary> Increment the value at the specified key by the specified increment, and then return it.
		/// 
		/// </summary>
		/// <param name="key">key where the data is stored
		/// </param>
		/// <param name="qty">how much to increment by
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns>the value after incrementing otherwise
		/// </returns>
		public virtual long Increment(string key, long qty, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return IncrDecrItem(IncrDecrCommandType.Increment, key, qty, sock);
		}
		
		/// <summary> Decrement the value at the specified key by 1, and then return it.
		/// 
		/// </summary>
		/// <param name="key">key where the data is stored
		/// </param>
		/// <returns> -1, if the key is not found, the value after incrementing otherwise
		/// </returns>
		public virtual long Decrement(string key)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return IncrDecrItem(IncrDecrCommandType.Decrement, key, 1, sock);
		}
		
		/// <summary> Decrement the value at the specified key by the specified increment, and then return it.
		/// 
		/// </summary>
		/// <param name="key">key where the data is stored
		/// </param>
		/// <param name="qty">how much to increment by
		/// </param>
		/// <param name="hashCode">if not null, then the int hashcode to use
		/// </param>
		/// <returns> -1, if the key is not found, the value after incrementing otherwise
		/// </returns>
		public virtual long Decrement(string key, long qty, int hashCode)
		{
            // get SockIO obj from hash or from key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return IncrDecrItem(IncrDecrCommandType.Decrement, key, qty, sock);
		}
		
		
		/// <summary> Retrieve a key from the server, using a specific hash.
		/// 
		/// If the data was compressed or serialized when compressed, it will automatically<br/>
		/// be decompressed or serialized, as appropriate. (Inclusive or)<br/>
		/// <br/>
		/// Non-serialized data will be returned as a string, so explicit conversion to<br/>
		/// numeric types will be necessary, if desired<br/>
		/// 
		/// </summary>
		/// <param name="key">key where data is stored
		/// </param>
		/// <returns> the object that was previously stored, or null if it was not previously stored
		/// </returns>
		public virtual object Get(string key)
		{
            // get SockIO obj using cache key
            SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return GetItem(key, sock);
        }
		
		/// <summary>
		/// Retrieve a key from the server, using a specific hash.
		/// 
		/// If the data was compressed or serialized when compressed, it will automatically<br/>
		/// be decompressed or serialized, as appropriate. (Inclusive or)<br/>
		/// <br/>
		/// Non-serialized data will be returned as a string, so explicit conversion to<br/>
		/// numeric types will be necessary, if desired<br/>
		/// </summary>
		/// 
		/// <param name="key">key where data is stored</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>\
		/// 
		/// <returns> the object that was previously stored, or null if it was not previously stored</returns>
		public virtual object Get(string key, int hashCode)
		{
			// get SockIO obj using cache key
			SockIOPool.SockIO sock = SockIOPool.Instance.GetSock(key, hashCode);

            if(sock == null)
                throw new SocketException("Unable to allocate socket");

            return GetItem(key, sock);
        }

        /// <summary>
        /// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
		/// is more efficient.
		/// </summary>
		/// 
		/// <param name="keys">String array of keys to retrieve</param>
		/// 
		/// <returns> Object array ordered in same order as key array containing results</returns>
		public virtual object[] GetMultiArray(string[] keys)
		{
			return GetMultiArray(keys, new int[0]);
		}
		
		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
		/// is more efficient.
		/// </summary>
		/// 
		/// <param name="keys">String array of keys to retrieve</param>
		/// <param name="hashCodes">if not null, then the Integer array of hashCodes</param>
		/// 
		/// <returns> Object array ordered in same order as key array containing results</returns>
		public virtual object[] GetMultiArray(string[] keys, int[] hashCodes)
		{
			IDictionary data = GetMulti(keys, hashCodes);
			
			object[] res = new object[keys.Length];
			for (int i = 0; i < keys.Length; i++)
			{
				res[i] = data[keys[i]];
			}
			
			return res;
		}
		
		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
		/// is more efficient.
		/// </summary>
		/// 
		/// <param name="keys">String array of keys to retrieve</param>
		/// 
		/// <returns>
		/// A hashmap with entries for each key is found by the server,
		/// keys that are not found are not entered into the hashmap, but attempting to
		/// retrieve them from the hashmap gives you null.
		/// </returns>
		public virtual IDictionary GetMulti(string[] keys)
		{
            return GetMulti(keys, new int[0]);
		}
		
		/// <summary>
		/// Retrieve multiple keys from the memcache.
		/// 
		/// This is recommended over repeated calls to {@link #get(String) get()}, since it<br/>
		/// is more efficient.
		/// </summary>
		/// 
		/// <param name="keys">keys to retrieve</param>
		/// <param name="hashCodes">if not null, then the Integer array of hashCodes</param>
		/// 
		/// <returns>
		/// A hashmap with entries for each key is found by the server,
		/// keys that are not found are not entered into the hashmap, but attempting to
		/// retrieve them from the hashmap gives you null.
		/// </returns>
		public virtual IDictionary GetMulti(string[] keys, int[] hashCodes)
		{
			IDictionary sockKeys = new Hashtable();
			
			for (int i = 0; i < keys.Length; ++i)
			{
                SockIOPool.SockIO sock = null;

                if(hashCodes.Length > i)
                {
                    // get SockIO obj from cache key
                    sock = SockIOPool.Instance.GetSock(keys[i], hashCodes[i]);
                }
                else
                {
                    sock = SockIOPool.Instance.GetSock(keys[i]);
                }

                if (sock == null)
					continue;
				
				// store in map and list if not already
				if (!sockKeys.Contains(sock.Host))
				{
					sockKeys.Add(sock.Host, new StringBuilder());
				}
				
				((StringBuilder)sockKeys[sock.Host]).Append(" " + keys[i]);
				
				// return to pool
				sock.Close();
			}
			
			if(log.IsInfoEnabled) log.Info("multi get socket count : " + sockKeys.Count);
			
			// now query memcache
			IDictionary ret = new Hashtable();
			
			foreach(string host in sockKeys.Keys)
			{
				// get SockIO obj from hostname
				SockIOPool.SockIO sock = SockIOPool.Instance.GetConnection(host);
				
				try
				{
					string cmd = "get" + ((StringBuilder) sockKeys[host]) + "\n";
					
                    if(log.IsDebugEnabled) log.Debug("memcache getMulti cmd: " + cmd);
					
                    sock.Write(UTF8Encoding.UTF8.GetBytes(cmd));
					sock.Flush();
					
                    LoadItems(sock, ret);
				}
				catch(IOException ioe)
				{
					// exception thrown
					if(log.IsErrorEnabled) log.Error("exception thrown while getting from cache on getMulti", ioe);
					
					// clear this sockIO obj from the list
					// and from the map containing keys
					sockKeys.Remove(host);

					try
					{
						sock.TrueClose();
					}
					catch(IOException ioee)
					{
						if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.GetHashCode(), ioee);
					}
					
					sock = null;
				}
				
				// Return socket to pool
				if (sock != null)
					sock.Close();
			}
			
			if(log.IsDebugEnabled) log.Debug("memcache: got back " + ret.Count + " results");
			
            return ret;
		}
		
		/// <summary> This method loads the data from cache into a Map.
		/// 
		/// Pass a SockIO object which is ready to receive data and a HashMap<br/>
		/// to store the results.
		/// 
		/// </summary>
		/// <param name="sock">socket waiting to pass back data
		/// </param>
		/// <param name="hm">hashmap to store data into
		/// </param>
		/// <throws>  IOException if io exception happens while reading from socket </throws>
		private void LoadItems(SockIOPool.SockIO sock, IDictionary hm)
		{
			while (true)
			{
				string line = sock.ReadLine();
				
				if(log.IsDebugEnabled) log.Debug("line: " + line);
				
				if (line.StartsWith(VALUE))
				{
					string[] info = line.Split(' ');
					string key = info[1];
					int flag = int.Parse(info[2]);
					int length = int.Parse(info[3]);
					
					if(log.IsDebugEnabled)
						log.Debug("key: " + key + "\n" +
							"flags: " + flag + "\n" +
							"length: " + length);
					
					// read obj into buffer
					byte[] buf = new byte[length];
					sock.Read(buf);
					sock.ClearEOL();
					
					// ready object
					object o;
					
					// check for compression
					if ((flag & F_COMPRESSED) != 0)
					{
						try
						{
							// read the input stream, and write to a byte array output stream since
							// we have to read into a byte array, but we don't know how large it
							// will need to be, and we don't want to resize it a bunch
							GZipInputStream gzi = new GZipInputStream(new MemoryStream(buf));
							MemoryStream bos = new MemoryStream(buf.Length);
							
							int count;
							byte[] tmp = new byte[2048];
							while ((count = gzi.Read(tmp, 0, tmp.Length)) != - 1)
							{
								bos.Write(tmp, 0, count);
							}
							
							// store uncompressed back to buffer
							buf = bos.ToArray();
							gzi.Close();
						}
						catch(IOException ioe)
						{
							if(log.IsErrorEnabled) log.Error("IOException thrown while trying to uncompress input stream for key: " + key, ioe);

							throw new IOException("IOException thrown while trying to uncompress input stream for key: " + key);
						}
					}
					
					// we can only take out serialized objects
					if ((flag & F_SERIALIZED) == 0)
					{
						if(log.IsInfoEnabled) log.Info("this object is not a serialized object.  Stuffing into a string.");

						o = new string(UTF8Encoding.UTF8.GetChars(buf));
					}
					else
					{
						// deserialize if the data is serialized
						BinaryReader ois = new BinaryReader(new MemoryStream(buf));
						try
						{
							o = new BinaryFormatter().Deserialize(ois.BaseStream);

							if(log.IsInfoEnabled) log.Info("deserializing " + o.GetType().FullName);
						}
						catch(Exception e)
						{
							if(log.IsErrorEnabled) log.Error("ClassNotFoundException thrown while trying to deserialize for key: " + key, e);
							
							throw new IOException("failed while trying to deserialize for key: " + key);
						}
					}
					
					// store the object into the cache
					hm.Add(key, o);
				}
				else if (END.Equals(line))
				{
					if(log.IsDebugEnabled) log.Debug("finished reading from cache server");
					
					break;
				}
			}
		}
		
		/// <summary> Invalidates the entire cache.
		/// 
		/// Will return true only if succeeds in clearing all servers.
		/// 
		/// </summary>
		/// <returns> success true/false
		/// </returns>
		public virtual bool FlushAll()
		{
			return FlushAll(null);
		}
		
		/// <summary> Invalidates the entire cache.
		/// 
		/// Will return true only if succeeds in clearing all servers.
		/// If pass in null, then will try to flush all servers.
		/// 
		/// </summary>
		/// <param name="servers">optional array of host(s) to flush (host:port)
		/// </param>
		/// <returns> success true/false
		/// </returns>
		public virtual bool FlushAll(string[] servers)
		{
			
			// get SockIOPool instance
			SockIOPool pool = SockIOPool.Instance;
			
			// return false if unable to get SockIO obj
			if (pool == null)
			{
				if(log.IsErrorEnabled) log.Error("unable to get SockIOPool instance");

				return false;
			}
			
			// get all servers and iterate over them
			servers = (servers == null)?pool.Servers:servers;
			
			// if no servers, then return early
			if (servers == null || servers.Length <= 0)
			{
				if(log.IsErrorEnabled) log.Error("no servers to flush");

				return false;
			}
			
			bool success = true;
			
			for (int i = 0; i < servers.Length; i++)
			{
				
				SockIOPool.SockIO sock = pool.GetConnection(servers[i]);
				if (sock == null)
				{
					if(log.IsErrorEnabled) log.Error("unable to get connection to : " + servers[i]);

					success = false;
					continue;
				}
				
				// build command
				string command = "flush_all\r\n";
				
				try
				{
					sock.Write(UTF8Encoding.UTF8.GetBytes(command));
					sock.Flush();
					
					// if we get appropriate response back, then we return true
					string line = sock.ReadLine();
					success = (OK.Equals(line))?success && true:false;
				}
				catch(IOException ioe)
				{
					if(log.IsErrorEnabled) log.Error("exception thrown while writing bytes to server on delete", ioe);
					
					try
					{
						sock.TrueClose();
					}
					catch(IOException ioee)
					{
						if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.ToString(), ioee);
					}
					
					success = false;
					sock = null;
				}
				
				if (sock != null)
					sock.Close();
			}
			
			return success;
		}
		
		/// <summary> Retrieves stats for all servers.
		/// 
		/// Returns a map keyed on the servername.
		/// The value is another map which contains stats
		/// with stat name as key and value as value.
		/// 
		/// </summary>
		/// <returns> Stats map
		/// </returns>
		public virtual IDictionary Stats()
		{
			return Stats(null);
		}
		
		/// <summary> Retrieves stats for passed in servers (or all servers).
		/// 
		/// Returns a map keyed on the servername.
		/// The value is another map which contains stats
		/// with stat name as key and value as value.
		/// 
		/// </summary>
		/// <param name="servers">string array of servers to retrieve stats from, or all if this is null
		/// </param>
		/// <returns> Stats map
		/// </returns>
		public virtual IDictionary Stats(string[] servers)
		{
			
			// get SockIOPool instance
			SockIOPool pool = SockIOPool.Instance;
			
			// return false if unable to get SockIO obj
			if (pool == null)
			{
				if(log.IsErrorEnabled) log.Error("unable to get SockIOPool instance");

				return null;
			}
			
			// get all servers and iterate over them
			servers = (servers == null)?pool.Servers:servers;
			
			// if no servers, then return early
			if (servers == null || servers.Length <= 0)
			{
				if(log.IsErrorEnabled) log.Error("no servers to check stats");

				return null;
			}
			
			// array of stats Maps
			IDictionary statsMaps = new Hashtable();
			
			for (int i = 0; i < servers.Length; i++)
			{
				
				SockIOPool.SockIO sock = pool.GetConnection(servers[i]);
				if (sock == null)
				{
					if(log.IsErrorEnabled) log.Error("unable to get connection to : " + servers[i]);

					continue;
				}
				
				// build command
				string command = "stats\r\n";
				
				try
				{
					sock.Write(UTF8Encoding.UTF8.GetBytes(command));
					sock.Flush();
					
					// map to hold key value pairs
					IDictionary stats = new Hashtable();

					// loop over results
					while (true)
					{
						string line = sock.ReadLine();
						
						if(log.IsDebugEnabled) log.Debug("line: " + line);
						
						if (line.StartsWith(STATS))
						{
							string[] info = line.Split(' ');
							string key = info[1];
							string value = info[2];
							
							if(log.IsDebugEnabled)
								log.Debug("key  : " + key + "\n" +
								"value: " + value);
							
							stats.Add(key, value);
						}
						else if (END.Equals(line))
						{
							// finish when we get end from server
							if(log.IsDebugEnabled) log.Debug("finished reading from cache server");

							break;
						}
						
						statsMaps.Add(servers[i], stats);
					}
				}
				catch(IOException ioe)
				{
					if(log.IsErrorEnabled) log.Error("exception thrown while writing bytes to server on delete", ioe);
					
					try
					{
						sock.TrueClose();
					}
					catch(IOException ioee)
					{
						if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.ToString(), ioee);
					}
					
					sock = null;
				}
				
				if (sock != null)
					sock.Close();
			}
			
			return statsMaps;
		}

        #region implementation

        /// <summary>
        /// Delete item
        /// </summary>
        /// <param name="key">Key to delete</param>
        /// <param name="expiry">Expiration to use</param>
        /// <param name="sock">SockIO to use</param>
        /// <returns><code>true</code> if successful, <code>fale</code> or exception otherwise</returns>
        private static bool DeleteItem(string key, DateTime expiry, SockIOPool.SockIO sock)
        {
            // Assumes non-null arguments

            // build command
            StringBuilder command = new StringBuilder();
            command.Append("delete ");
            command.Append(key);
            if(!expiry.Equals(DateTime.MinValue))
            {
                command.Append(" ");
                command.Append(expiry.Ticks / 1000);
            }
            command.Append("\n");

            try
            {
                sock.Write(UTF8Encoding.UTF8.GetBytes(command.ToString()));
                sock.Flush();

                // if we get appropriate response back, then we return true
                string line = sock.ReadLine();

                switch(line)
                {
                    case DELETED:
                        return true;
                    case NOTFOUND:
#if EXCEPTIONS
                        throw new NotFoundException();
#else
                        return false;
#endif
                    default:
#if EXCEPTIONS
                        throw new MemcachedException(line);
#else
                        return false;
#endif
                }
            }
            catch(IOException ioe)
            {
                if(log.IsErrorEnabled) log.Error("exception thrown while writing bytes to server on delete", ioe);

                try
                {
                    sock.TrueClose();
                }
                catch(IOException ioee)
                {
                    if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.ToString(), ioee);
                }

                sock = null;

#if EXCEPTIONS
                throw new SocketException(e);
#else
                return false;
#endif
            }
            finally
            {
                if(sock != null)
                    sock.Close();
            }
        }

        /// <summary>
        /// Stores data to cache.
        /// 
        /// If data does not already exist for this key on the server, or if the key is being<br/>
        /// deleted, the specified value will not be stored.<br/>
        /// The server will automatically delete the value when the expiration time has been reached.<br/>
        /// <br/>
        /// If compression is enabled, and the data is longer than the compression threshold<br/>
        /// the data will be stored in compressed form.<br/>
        /// <br/>
        /// As of the current release, all objects stored will use serialization.
        /// </summary>
        /// 
        /// <param name="cmdType">action to take (set, add, replace)</param>
        /// <param name="key">key to store cache under</param>
        /// <param name="value">object to cache</param>
        /// <param name="expiry">expiration, if any</param>
        /// <param name="sock">SockIO to use</param>
        /// 
        /// <returns><code>true</code> if successful, <code>fale</code> or exception otherwise</returns>
        private bool SetItem(SetCommandType cmdType, string key, object value, DateTime expiry, SockIOPool.SockIO sock)
		{		
			// store flags
			int flags = 0;
			
			// byte array to hold data
			byte[] val;

            if(expiry.Equals(DateTime.MinValue))
                expiry = new DateTime(0);

            // serialize the object
			// unless client request data to be not serialized
			// in which case, we will go with string representation
			// but only for a select few classes
			if (!(value is string || value is Int32 || value is Double || value is Single || value is Int64 || value is SByte || value is Int16 || value is Char || value is StringBuilder))
			{
				// we can ONLY not serialize the above classes
				// this is generally a bad idea as we should always
				// just use serialization.
				// but, it is useful for sharing data between non-.NET
				// and also for storing ints for the increment method
				if(log.IsInfoEnabled) log.Info("storing data as a string for key: " + key + " for class: " + value.GetType().FullName);
				
                val = UTF8Encoding.UTF8.GetBytes(value.ToString());
			}
			else
			{
				if(log.IsInfoEnabled) log.Info("serializing for key: " + key + " for class: " + value.GetType().FullName);
				try
				{
					MemoryStream bos = new MemoryStream();
					new BinaryFormatter().Serialize((new BinaryWriter(bos)).BaseStream, value);

					val = bos.ToArray();
					flags |= F_SERIALIZED;
				}
				catch(SerializationException se)
				{
					if(log.IsErrorEnabled) log.Error("failed to serialize obj: " + value.ToString(), se);
					
					// return socket to pool and bail
					sock.Close();

#if EXCEPTIONS
                    throw new MemcachedException(se);
#else
                    return false;
#endif
                }
			}
			
			// now try to compress if we want to
			// and if the length is over the threshold 
			if (compressEnable && val.Length > compressThreshold)
			{
				if(log.IsInfoEnabled) log.Info("trying to compress data, size prior to compression: " + val.Length);
				
				try
				{
					MemoryStream bos = new MemoryStream(val.Length);
					GZipOutputStream gos = new GZipOutputStream(bos);
					gos.Write(val, 0, val.Length);
					gos.Close();
					
					// store it and set compression flag
					val = bos.ToArray();
					flags |= F_COMPRESSED;
					
					if(log.IsInfoEnabled) log.Info("compression succeeded, size after: " + val.Length);
				}
				// TODO Not sure which specific exception(s) is/are thrown
                catch(Exception e)
				{
					if(log.IsErrorEnabled) log.Error("Exception while compressing stream", e);
				}
			}
			
			// now write the data to the cache server
            StringBuilder cmd = new StringBuilder();
            switch(cmdType)
            {
                case SetCommandType.Add:
                    cmd.Append("add");
                    break;
                case SetCommandType.Replace:
                    cmd.Append("replace");
                    break;
                case SetCommandType.Set:
                    cmd.Append("set");
                    break;
                default:
                    // As if this can actually happen
                    throw new ArgumentException("cmdType");
            }
            cmd.Append(" ");
            cmd.Append(key);
            cmd.Append(" ");
            cmd.Append(flags.ToString());
            cmd.Append(" ");
            cmd.Append(expiry.Ticks / 1000);
            cmd.Append(" ");
            cmd.Append(val.Length.ToString());
            cmd.Append("\n");

            try
			{				
                sock.Write(UTF8Encoding.UTF8.GetBytes(cmd.ToString()));
			    sock.Write(val);
			    sock.Write(UTF8Encoding.UTF8.GetBytes("\n"));
			    sock.Flush();

                // get result code
				string line = sock.ReadLine();
				if(log.IsInfoEnabled) log.Info("memcache cmd (result code): " + cmd + " (" + line + ")");
				
				switch(line)
                {
                    case STORED:
                        if(log.IsInfoEnabled) log.Info("data successfully stored for key: " + key);
					    return true;
                    case NOTSTORED:
                        if(log.IsInfoEnabled) log.Info("data not stored in cache for key: " + key);
#if EXCEPTIONS
                        throw new NotStoredException(key);
#else
                        return false;
#endif
                    default:
					    if(log.IsErrorEnabled) log.Error("error storing data in cache for key: " + key + " -- length: " + val.Length);
#if EXCEPTIONS
                        throw new MemcachedException(line);
#else
                        return false;
#endif
                }
			}
            catch(IOException ioe)
            {
                if(log.IsErrorEnabled) log.Error("exception thrown while writing bytes to server on set", ioe);

                try
                {
                    sock.TrueClose();
                }
                catch(IOException ioee)
                {
                    if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.ToString(), ioee);
                }

                sock = null;
#if EXCEPTIONS
                throw new SocketException(e);
#else
                return false;
#endif
            }
            finally
            {
                if(sock != null)
                    sock.Close();
            }
		}

        /// <summary>
        /// Get item from cache
        /// </summary>
        /// <param name="key">Key of desired item</param>
        /// <param name="sock">Socket to use</param>
        /// <returns>Desired object</returns>
        private object GetItem(string key, SockIOPool.SockIO sock)
        {
            try
            {
                StringBuilder cmd = new StringBuilder();
                cmd.Append("get ");
                cmd.Append(key);
                cmd.Append("\n");
                
                if(log.IsDebugEnabled) log.Debug("memcache get command: " + cmd);

                sock.Write(UTF8Encoding.UTF8.GetBytes(cmd.ToString()));
                sock.Flush();

                // build empty map
                // and fill it from server
                IDictionary hm = new Hashtable();
                LoadItems(sock, hm);

                // debug code
                if(log.IsDebugEnabled) log.Debug("memcache: got back " + hm.Count + " results");

                // return the value for this key if we found it
                // else return null 
                
                object result = hm[key];

                if(result == null)
#if EXCEPTIONS
                    throw new NotFoundException(key);
#else
                    return false;
#endif

                return result;
            }
            catch(IOException ioe)
            {
                if(log.IsErrorEnabled) log.Error("exception thrown while trying to get object from cache for key: " + key, ioe);

                try
                {
                    sock.TrueClose();
                }
                catch(IOException ioee)
                {
                    if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.ToString(), ioee);
                }

                sock = null;

#if EXCEPTIONS
                throw new SocketException(e);
#else
                return false;
#endif
            }
            finally
            {
                if(sock != null)
                    sock.Close();
            }
        }

        /// <summary>
        /// Increments/decrements the value at the specified key by inc.
        /// 
        /// From Javadocs:
        /// Note that the server uses a 32-bit unsigned integer, and checks for<br/>
        /// underflow. In the event of underflow, the result will be zero.  Because<br/>
        /// java lacks unsigned types, the value is returned as a 64-bit integer.<br/>
        /// The server will only decrement a value if it already exists;<br/>
        /// if a value is not found, -1 will be returned.
        /// 
        /// .NET supports unsigned types so refactor
        /// </summary>
        /// 
        /// <param name="cmdType">increment/decrement type</param>
        /// <param name="key">cache key</param>
        /// <param name="qty">amount to incr or decr</param>
        /// <param name="sock">SockIO to use</param>
        /// 
        /// <returns> new value or -1 if not exist</returns>
        private long IncrDecrItem(IncrDecrCommandType cmdType, string key, long qty, SockIOPool.SockIO sock)
        {
            // Assumes non-null arguments

            if(log.IsDebugEnabled) log.Debug("memcache incr/decr command: " + cmdType.ToString());

            // now write the data to the cache server
            StringBuilder cmd = new StringBuilder();
            switch(cmdType)
            {
                case IncrDecrCommandType.Increment:
                    cmd.Append("incr");
                    break;
                case IncrDecrCommandType.Decrement:
                    cmd.Append("decr");
                    break;
                default:
                    // As if this can actually happen
                    throw new ArgumentException("cmdType");
            }
            cmd.Append(" ");
            cmd.Append(key);
            cmd.Append(" ");
            cmd.Append(qty.ToString());
            cmd.Append("\n");

            try
            {
                sock.Write(UTF8Encoding.UTF8.GetBytes(cmd.ToString()));
                sock.Flush();

                // get result back
                string line = sock.ReadLine();

                if(Regex.IsMatch("[^0-9-]", line))
                {
                    return long.Parse(line);
                }
                else if(NOTFOUND.Equals(line))
                {
                    if(log.IsInfoEnabled) log.Info("key not found to incr/decr for key: " + key);
#if EXCEPTIONS
                    throw new NotFoundException();
#else
                    // TODO Hacky
                    return -1;
#endif
                }

                if(log.IsErrorEnabled) log.Error("error incr/decr key: " + key);
#if EXCEPTIONS
                throw new MemcachedException(cmd.ToString());
#else
                // TODO Hacky
                return -1;
#endif
            }
            catch(IOException ioe)
            {
                // exception thrown
                if(log.IsErrorEnabled) log.Error("exception thrown while trying to incr/decr", ioe);

                try
                {
                    sock.TrueClose();
                }
                catch(IOException ioee)
                {
                    if(log.IsErrorEnabled) log.Error("failed to close socket : " + sock.ToString(), ioee);
                }

                sock = null;

#if EXCEPTIONS
                throw new SocketException(e);
#else
                return -1;
#endif
            }
            finally
            {
                if(sock != null)
                    sock.Close();
            }
        }

        #endregion

        // return codes
        private const string VALUE = "VALUE"; // start of value line from server
        private const string STATS = "STAT"; // start of stats line from server
        private const string DELETED = "DELETED"; // successful deletion
        private const string NOTFOUND = "NOT_FOUND"; // record not found for delete or incr/decr
        private const string STORED = "STORED"; // successful store of data
        private const string NOTSTORED = "NOT_STORED"; // data not stored
        private const string OK = "OK"; // success
        private const string END = "END"; // end of data from server
        private const string ERROR = "ERROR"; // invalid command name from client
        
        //private const string CLIENT_ERROR = "CLIENT_ERROR"; // client error in input line - invalid protocol
        //private const string SERVER_ERROR = "SERVER_ERROR"; // server error

        // values for cache flags 
        //
        // using 8 (1 << 3) so other clients don't try to unpickle/unstore/whatever
        // things that are serialized... I don't think they'd like it. :)
        private const int F_COMPRESSED = 2;
        private const int F_SERIALIZED = 8;

        // flags
        private bool serialize;
        private bool compressEnable;
        private long compressThreshold;
    }
}