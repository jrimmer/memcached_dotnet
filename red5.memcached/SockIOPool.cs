//
// MemCached .NET client, connection pool for Socket IO
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
using System.Collections.Specialized;
using System.IO;
using System.Threading;
using System.Net.Sockets;
using System.Text;

using log4net;
using log4net.Config;

using ICSharpCode.SharpZipLib.Checksums;

namespace red5.memcached
{
	/// <summary> This class is a connection pool for maintaning a pool of persistent connections<br/>
	/// to memcached servers.
	/// 
	/// The pool must be initialized prior to use. This should typically be early on<br/>
	/// in the lifecycle of the server instance.<br/>
	/// <br/>
	/// <h3>An example of initializing using defaults:</h3>
	/// <pre>
	/// 
	/// static {
	/// String[] serverlist = { "cache0.server.com:12345", "cache1.server.com:12345" };
	/// 
	/// SockIOPool pool = SockIOPool.getInstance();
	/// pool.setServers(serverlist);
	/// pool.initialize();	
	/// }
	/// </pre> 
	/// <h3>An example of initializing using defaults and providing weights for servers:</h3>
	/// <pre>
	/// static {
	/// String[] serverlist = { "cache0.server.com:12345", "cache1.server.com:12345" };
	/// Integer[] weights   = { new Integer(5), new Integer(2) };
	/// 
	/// SockIOPool pool = SockIOPool.getInstance();
	/// pool.setServers(serverlist);
	/// pool.setWeights(weights);	
	/// pool.initialize();	
	/// }
	/// </pre> 
	/// <h3>An example of initializing overriding defaults:</h3>
	/// <pre>
	/// static {
	/// String[] serverlist     = { "cache0.server.com:12345", "cache1.server.com:12345" };
	/// Integer[] weights       = { new Integer(5), new Integer(2) };	
	/// int initialConnections  = 10;
	/// int minSpareConnections = 5;
	/// int maxSpareConnections = 50;	
	/// long maxIdleTime        = 1000 * 60 * 30;	// 30 minutes
	/// long maintThreadSleep   = 1000 * 5;			// 5 seconds
	/// int	socketTimeOut       = 1000 * 3;			// 3 seconds to block on reads
	/// boolean failover        = false;			// turn off auto-failover in event of server down	
	/// boolean nagleAlg        = false;			// turn off Nagle's algorithm on all sockets in pool	
	/// 
	/// SockIOPool pool = SockIOPool.getInstance();
	/// pool.setServers(serverlist);
	/// pool.setWeights(weights);	
	/// pool.setInitConn(initialConnections);
	/// pool.setMinConn(minSpareConnections);
	/// pool.setMaxConn(maxSpareConnections);
	/// pool.setMaxIdle(maxIdleTime);
	/// pool.setMaintSleep(maintThreadSleep);
	/// pool.setSocketTO(socketTimeOut);	
	/// pool.setNagle(nagleAlg);	
	/// pool.setHashingAlg(SockIOPool.NEW_COMPAT_HASH);	
	/// pool.initialize();	
	/// }
	/// </pre> 
	/// The easiest manner in which to initialize the pool is to set the servers and rely on defaults as in the first example.<br/> 
	/// After pool is initialized, a client will request a SockIO object by calling getSock with the cache key<br/>
	/// The client must always close the SockIO object when finished, which will return the connection back to the pool.<br/> 
	/// <h3>An example of retrieving a SockIO object:</h3>
	/// <pre>
	/// SockIOPool.SockIO sock = SockIOPool.getInstance().getSock(key);
	/// try {
	/// sock.write("version\r\n");	
	/// sock.flush();	
	/// System.out.println("Version: " + sock.readLine());	
	/// }
	/// catch(IOException ioe) { System.out.println("io exception thrown") };	
	/// 
	/// sock.close();	
	/// </pre> 
	/// 
	/// </summary>
	public class SockIOPool
	{
		private static readonly ILog log = LogManager.GetLogger(typeof(SockIOPool).FullName);

		static SockIOPool()
		{
			DOMConfigurator.Configure(new System.IO.FileInfo("./dotnet-memcached.config"));
		}

		/// <summary>
        /// Hash type
        /// </summary>
        public enum HASH_TYPE
        {
            /// <summary>
            /// string.GetHashCode();
            /// </summary>
            NATIVE,
            /// <summary>
            /// Original compatibility hashing algorithm (works with other clients)
            /// </summary>
            OLD_COMPAT,
            /// <summary>
            /// New CRC32 based compatibility hashing algorithm (works with other clients)
            /// </summary>
            NEW_COMPAT
        }

        /// <summary>
        /// Retrieve instance
        /// </summary>
        public static SockIOPool Instance
		{
			// this pool is a singleton
			get
			{
				return instance;
			}
		}

       
        /// <summary>
        /// Gets and sets the current list of all cache servers. 
        /// </summary>
        /// 
        /// <value>String array of servers [host:port]</value>
        virtual public string[] Servers
		{
			get
			{
				return servers;
			}
			
			set
			{
				servers = value;
			}
		}

		/// <summary>
        /// Gets and sets the current list of weights. 
        /// 
        /// This is an int array with each element corresponding to an element
        /// in the same position in the server String array. 
        /// </summary>
        /// 
        /// <value>Integer array of weights</value>
        virtual public int[] Weights
		{
			get
			{
				return this.weights;
			}
			
			set
			{
				this.weights = value;
			}
		}

		/// <summary>
        /// Gets and sets the current setting for the initial number of connections per server in
        /// the available pool. 
        /// </summary>
        /// 
        /// <value>Number of connections</value>
        virtual public int InitConn
		{
			get
			{
				return this.initConn;
			}
			
			set
			{
				this.initConn = value;
			}
		}

		/// <summary>
		/// The minimum number of spare connections in available pool. 
		/// </summary>
		/// 
		/// <value>number of connections</value>
		virtual public int MinConn
		{
			get
			{
				return this.minConn;
			}
			
			set
			{
				this.minConn = value;
			}
		}

		/// <summary>
		/// Returns the maximum number of spare connections allowed in available pool. 
		/// </summary>
		/// 
        /// <value>number of connections</value>
		virtual public int MaxConn
		{
			get
			{
				return this.maxConn;
			}
			
			set
			{
				this.maxConn = value;
			}
		}

		/// <summary>
		/// Current max idle setting. 
		/// </summary>
		/// 
		/// <value>idle time in ms</value>
		virtual public long MaxIdle
		{
			get
			{
				return this.maxIdle;
			}
			
			set
			{
				this.maxIdle = value;
			}
		}

		/// <summary>
		/// The current maint thread sleep time in ms.
		/// 
		/// The sleep time between runs of the pool maintenance thread.
		/// If set to 0, then the maint thread will not be started. 
		/// </summary>
		/// 
		/// <value>sleep time in ms</value>
		virtual public long MaintSleep
		{
			get
			{
				return this.maintSleep;
			}
			
			set
			{
				this.maintSleep = value;
			}
		}

		/// <summary>
		/// The socket timeout for reads in ms.
		/// </summary>
		/// 
		/// <value>timeout in ms</value>
		virtual public int SocketTO
		{
			get
			{
				return this.socketTO;
			}
			
			set
			{
				this.socketTO = value;
			}
		}

		/// <summary>
		/// Current state of failover flag for the pool.
		/// 
        /// If this flag is set to true, and a socket fails to connect,<br/>
        /// the pool will attempt to return a socket from another server<br/>
        /// if one exists.  If set to false, then getting a socket<br/>
        /// will return null if it fails to connect to the requested server.
        /// </summary>
        /// 
		/// <value>true/false</value>
		virtual public bool Failover
		{
			get
			{
				return this.failover;
			}
			
			set
			{
				this.failover = value;
			}
		}

		/// <summary>
		/// Current status of nagle flag for the pool
		/// 
		/// If false, will turn off Nagle's algorithm on all sockets created.
		/// </summary>
		/// 
		/// <value>true/false</value>
		virtual public bool Nagle
		{
			get
			{
				return this.nagle;
			}
			
			set
			{
				this.nagle = value;
			}
		}

		/// <summary>
		/// The hashing algorithm we will use.
		/// 
		/// The types are as follows.
		/// 
		/// SockIOPool.NATIVE_HASH     - native String.hashCode() - fast (cached) but not compatible with other clients
		/// SockIOPool.OLD_COMPAT_HASH - original compatibility hashing alg (works with other clients)
		/// SockIOPool.NEW_COMPAT_HASH - new CRC32 based compatibility hashing algorithm (fast and works with other clients)
		/// </summary>
		/// 
		/// <value>Hashing algorithm</value>
		virtual public HASH_TYPE HashingAlg
		{
			get
			{
				return this.hashingAlg;
			}
			
			set
			{
				this.hashingAlg = value;
			}
		}

        /// <summary>
        /// Returns state of pool. 
		/// </summary>
		/// <returns>
		///     <CODE>true</CODE> if initialized.
		/// </returns>
		virtual public bool Initialized
		{
			get
			{
				return initialized;
			}
		}
		
		// static pool instance
		private static SockIOPool instance = new SockIOPool();
				
		// Pool data
		private bool initialized = false;
		private bool maintThreadRunning = false;
		private int maxCreate = 1; // this will be initialized by pool when the pool is initialized
		private IDictionary createShift;
		
		// initial, min and max pool sizes
		private int poolMultiplier = 4;
		private int initConn = 3;
		private int minConn = 3;
		private int maxConn = 10;
		private long maxIdle = 1000 * 60 * 3; // max idle time for avail sockets
		private long maintSleep = 1000 * 5; // maintenance thread sleep time
		private int socketTO = 1000 * 10; // default timeout of socket connections on reads
		private bool failover = true; // default to failover in event of cache server dead
		private bool nagle = true; // enable/disable Nagle's algorithm
		private HASH_TYPE hashingAlg = HASH_TYPE.NEW_COMPAT; // default to using the native hash as it is the fastest
		
		// list of all servers
		private string[] servers;
		private int[] weights;
		private IList buckets;
		
		// dead server map
		private IDictionary hostDead;
		private IDictionary hostDeadDur;
		
		// map to hold all available sockets
		private IDictionary availPool;
		
		// map to hold busy sockets
		private IDictionary busyPool;
		
		/// <summary>
		/// Empty constructor
		/// </summary>
		protected internal SockIOPool()
		{
		}
		
		/// <summary>
		/// Initializes the pool.
		/// </summary>
		public virtual void Initialize()
		{
			lock (this)
			{
				
				// check to see if already initialized
				if (initialized && (buckets != null) && (availPool != null) && (busyPool != null))
				{
					if(log.IsErrorEnabled) log.Error("Trying to initialize an already initialized pool");
					return ;
				}
				
				// initialize empty maps
				buckets = new ArrayList();
				availPool = Hashtable.Synchronized(new Hashtable(servers.Length * initConn));
				busyPool = Hashtable.Synchronized(new Hashtable(servers.Length * initConn));
				hostDeadDur = Hashtable.Synchronized(new Hashtable());
				hostDead = Hashtable.Synchronized(new Hashtable());
				createShift = Hashtable.Synchronized(new Hashtable());
				maxCreate = (poolMultiplier > minConn)?minConn:minConn / poolMultiplier; // only create up to maxCreate connections at once
				
				if(log.IsInfoEnabled)
					log.Info("Initializing pool with following settings:\n" +
					"initial size: " + initConn + "\n" +
					"min spare   : " + minConn + "\n" +
					"max spare   : " + maxConn);
				
				// if servers is not set, or it empty, then
				// throw a runtime exception
				if (servers == null || servers.Length <= 0)
				{
					if(log.IsErrorEnabled) log.Error("Trying to initialize with no servers");
					
					throw new SystemException("Trying to initialize with no servers");
				}
				
				for (int i = 0; i < servers.Length; i++)
				{
					
					// add to bucket
					// with weights if we have them 
					if (weights != null && weights.Length > i)
					{
						for (int k = 0; k < weights[i]; k++)
						{
							buckets.Add(servers[i]);
							if(log.IsDebugEnabled) log.Debug("Added " + servers[i] + " to server bucket");
						}
					}
					else
					{
						buckets.Add(servers[i]);
						if(log.IsDebugEnabled) log.Debug("Added " + servers[i] + " to server bucket");
					}
					
					// create initial connections
					if(log.IsDebugEnabled) log.Debug("Creating initial connections (" + initConn + ") for host: " + servers[i]);
					
					for (int j = 0; j < initConn; j++)
					{
						SockIO socket = CreateSocket(servers[i]);
						if (socket == null)
							break;
						
						AddSocketToPool(availPool, servers[i], socket);
						if(log.IsDebugEnabled) log.Debug("created and added socket: " + socket.GetHashCode() + " for host " + servers[i]);
					}
				}
				
				// mark pool as initialized
				this.initialized = true;
				
				// start maint thread
				if (this.maintSleep > 0)
					this.StartMaintThread();
			}
		}
		
		/// <summary> Creates a new SockIO obj for the given server.
		/// 
		/// If server fails to connect, then return null and do not try<br/>
		/// again until a duration has passed.  This duration will grow<br/>
		/// by doubling after each failed attempt to connect. 
		/// </summary>
		/// 
		/// <param name="host">host:port to connect to</param>
		/// <returns>SockIO obj or null if failed to create</returns>
		private SockIO CreateSocket(string host)
		{
			SockIO socket = null;
			
			// if host is dead, then we don't need to try again
			// until the dead status has expired
			if (hostDead.Contains(host) && hostDeadDur.Contains(host))
			{
				DateTime store = (DateTime) hostDead[host];
				long expire = ((long) hostDeadDur[host]);
				
				if ((store.Ticks + expire) > (DateTime.Now.Ticks - 621355968000000000) / 10000)
					return null;
			}
			
			try
			{
				socket = new SockIO(host, this.socketTO, this.nagle);
				
				if (!socket.Connected)
				{
					if(log.IsErrorEnabled) log.Error("failed to get SockIO obj for: " + host + " -- new socket is not connected");
					try
					{
						socket.TrueClose();
					}
					catch(Exception ex)
					{
						if(log.IsErrorEnabled) log.Error("failed to close SockIO obj for server: " + host, ex);

						socket = null;
					}
				}
			}
			catch(Exception ex)
			{
				if(log.IsErrorEnabled) log.Error("failed to get SockIO obj for: " + host, ex);
			}
			
			// if we failed to get socket, then mark
			// host dead for a duration which falls off
			if (socket == null)
			{
				DateTime now = DateTime.Now;
				hostDead.Add(host, now);
				long expire = (hostDeadDur.Contains(host))?((long) ((long) hostDeadDur[host]) * 2):1000;
				hostDeadDur.Add(host, (long) expire);
				if(log.IsDebugEnabled) log.Debug("Ignoring dead host: " + host + " for " + expire + " ms");
				
				// also clear all entries for this host from availPool
				ClearHostFromPool(availPool, host);
			}
			else
			{
				if(log.IsDebugEnabled) log.Debug("Created socket (" + socket.GetHashCode() + ") for host: " + host);
				hostDead.Remove(host);
				hostDeadDur.Remove(host);
			}
			
			return socket;
		}
		
		/// <summary>
		/// Returns appropriate SockIO object given
		/// string cache key.
		/// </summary>
		/// 
		/// <param name="key">hashcode for cache key</param>
		/// 
		/// <returns> SockIO obj connected to server</returns>
		public virtual SockIO GetSock(string key)
		{
			// if no servers return null
			if (buckets.Count == 0)
				return null;
			
			// if only one server, return it
			if (buckets.Count == 1)
				return GetConnection((string) buckets[0]);

			int hv;

            switch(hashingAlg)
            {
                case HASH_TYPE.NATIVE:
                    hv = key.GetHashCode();
                    break;
                case HASH_TYPE.OLD_COMPAT:
                    hv = OrigCompatHashingAlg(key);
                    break;
                case HASH_TYPE.NEW_COMPAT:
                    hv = NewCompatHashingAlg(key);
                    break;
                default:
                    // use the native hash as a default
                    hv = key.GetHashCode();
                    break;
            }

            return GetSock(key, hv);
        }
		
		/// <summary>
		/// Returns appropriate SockIO object given
		/// string cache key and optional hashcode.
		/// 
		/// Trys to get SockIO from pool.  Fails over
		/// to additional pools in event of server failure.
		/// </summary>
		/// 
		/// <param name="key">hashcode for cache key</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// 
		/// <returns> SockIO obj connected to server</returns>
		public virtual SockIO GetSock(string key, int hashCode)
		{
			if(log.IsInfoEnabled) log.Info("cache socket pick " + key + " " + hashCode);
			
			if (!this.initialized)
			{
				if(log.IsErrorEnabled) log.Error("attempting to get SockIO from uninitialized pool!");
				return null;
			}
			
			// if no servers return null
			if (buckets.Count == 0)
				return null;
			
			// if only one server, return it
			if (buckets.Count == 1)
				return GetConnection((string) buckets[0]);
			
			int tries = 0;
			
			// generate hashcode
			int hv = hashCode;
			
			// keep trying different servers until we find one
			int bucketSize = buckets.Count;
			
            while (tries++ < bucketSize)
			{
				// get bucket using hashcode 
				// get one from factory
				int bucket = hv % bucketSize;
				if(bucket < 0)
					bucket += bucketSize;
				
				SockIO sock = GetConnection((string) buckets[bucket]);
				
				if(log.IsInfoEnabled) log.Info("cache choose " + buckets[bucket] + " for " + key);
				
				if (sock != null)
					return sock;
				
				// if we do not want to failover, then bail here
				if (!failover)
					return null;
				
				// if we failed to get a socket from this server
				// then we try again by adding an incrementer to the
				// current hash and then rehashing 
				hv += ("" + hv + tries).GetHashCode();
			}
			
			return null;
		}
		
		/// <summary>
		/// Internal private hashing method.
		/// 
		/// This is the original hashing algorithm from other clients.
		/// Found to be slow and have poor distribution.
		/// </summary>
		/// 
		/// <param name="key">String to hash</param>
		/// 
		/// <returns> hashCode for this string using our own hashing algorithm</returns>
		private static int OrigCompatHashingAlg(string key)
		{
			int hash = 0;
			char[] cArr = key.ToCharArray();
			
			for (int i = 0; i < cArr.Length; ++i)
			{
				hash = (hash * 33) + cArr[i];
			}
			
			return hash;
		}
		
		/// <summary>
		/// Internal private hashing method.
		/// 
		/// This is the new hashing algorithm from other clients.
		/// Found to be fast and have very good distribution. 
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		private static int NewCompatHashingAlg(string key)
		{
			Crc32 checksum = new Crc32();

			checksum.Update(UTF8Encoding.UTF8.GetBytes(key));

			int crc = (int) checksum.Value;
			
			return (crc >> 16) & 0x7fff;
		}
		
		/// <summary>
		/// Returns a SockIO object from the pool for the passed in host.
		/// 
		/// Meant to be called from a more intelligent method<br/>
		/// which handles choosing appropriate server<br/>
		/// and failover. 
		/// </summary>
		/// 
		/// <param name="host">host from which to retrieve object</param>
		/// 
		/// <returns> SockIO object or null if fail to retrieve one</returns>
		public virtual SockIO GetConnection(string host)
		{
			lock (this)
			{
				
				if (!this.initialized)
				{
					if(log.IsErrorEnabled) log.Error("attempting to get SockIO from uninitialized pool!");
					return null;
				}
				
				if (host == null)
					return null;
				
				// if we have items in the pool
				// then we can return it
				if (availPool != null && !(availPool.Count == 0) )
				{
					
					// take first connected socket
					IDictionary aSockets = (IDictionary) availPool[host];
					if (aSockets != null && !(aSockets.Count == 0) )
					{
						foreach(SockIO socket in aSockets.Keys)
						{
							if (socket.Connected)
							{
								if(log.IsDebugEnabled) log.Debug("moving socket for host (" + host + ") to busy pool ... socket: " + socket);
								
								// remove from avail pool
								aSockets.Remove(socket);
								
								// add to busy pool
								AddSocketToPool(busyPool, host, socket);
								
								// return socket
								return socket;
							}
							else
							{
								// not connected, so we need to remove it
								if(log.IsErrorEnabled) log.Error("socket in avail pool is not connected: " + socket.GetHashCode() + " for host: " + host);

								// remove from avail pool
								aSockets.Remove(socket);
							}
						}
					}
				}
				
				// if here, then we found no sockets in the pool
				// try to create on a sliding scale up to maxCreate		
                int shift = 0;
                if(createShift.Count > 0)
                    shift = (int) createShift[host];
				
				int create = 1 << shift;
				if (create >= maxCreate)
				{
					create = maxCreate;
				}
				else
				{
					shift++;
				}
				
				// store the shift value for this host
				createShift.Add(host, (int) shift);
				
				if(log.IsDebugEnabled) log.Debug("creating " + create + " new SockIO objects");
				
				for (int i = create; i > 0; i--)
				{
					SockIO socket = CreateSocket(host);
					if (socket == null)
						break;
					
					if (i == 1)
					{
						// last iteration, add to busy pool and return sockio
						AddSocketToPool(busyPool, host, socket);
						return socket;
					}
					else
					{
						// add to avail pool
						AddSocketToPool(availPool, host, socket);
					}
				}
				
				// should never get here
				return null;
			}
		}
		
		/// <summary>
		/// Adds a socket to a given pool for the given host.
		/// 
		/// Internal utility method. 
		/// </summary>
		/// 
		/// <param name="pool">pool to add to</param>
		/// <param name="host">host this socket is connected to</param>
		/// <param name="socket">socket to add</param>
		private void AddSocketToPool(IDictionary pool, string host, SockIO socket)
		{
			lock (this)
			{
				
				if (pool.Contains(host))
				{
					IDictionary sockets = (IDictionary) pool[host];
					if (sockets != null)
					{
						sockets.Add(socket, (long) ((DateTime.Now.Ticks - 621355968000000000) / 10000));
						return ;
					}
				}
				
				IDictionary sockets2 = Hashtable.Synchronized(new Hashtable());
				sockets2.Add(socket, (long) ((DateTime.Now.Ticks - 621355968000000000) / 10000));
				pool.Add(host, sockets2);
			}
		}
		
		/// <summary>
		/// Removes a socket from specified pool for host.
		/// 
		/// Internal utility method. 
		/// </summary>
		/// 
		/// <param name="pool">pool to remove from</param>
		/// <param name="host">host pool</param>
		/// <param name="socket">socket to remove</param>
		private void RemoveSocketFromPool(IDictionary pool, string host, SockIO socket)
		{
			lock (this)
			{
				if (pool.Contains(host))
				{
					IDictionary sockets = (IDictionary) pool[host];
					if (sockets != null)
					{
						sockets.Remove(socket);
					}
				}
			}
		}
		
		/// <summary>
		/// Closes and removes all sockets from specified pool for host. 
		/// 
		/// Internal utility method. 
		/// </summary>
		/// 
		/// <param name="pool">pool to clear</param>
		/// <param name="host">host to clear</param>
		private void ClearHostFromPool(IDictionary pool, string host)
		{
			lock (this)
			{
				if (pool.Contains(host))
				{
					IDictionary sockets = (IDictionary) pool[host];
					if (sockets != null && sockets.Count > 0)
					{
						foreach(SockIO socket in sockets.Keys)
						//for (Iterator i = sockets.keySet().iterator(); i.hasNext(); )
						{
							//SockIO socket = (SockIO) i.next();
							try
							{
								socket.TrueClose();
							}
							catch(IOException ioe)
							{
								if(log.IsErrorEnabled) log.Error("failed to close socket", ioe);
							}
							
							sockets.Remove(socket);
							//socket = null;
						}
					}
				}
			}
		}
		
		/// <summary>
		/// Checks a SockIO object in with the pool.
		/// 
		/// This will remove SocketIO from busy pool, and optionally<br/>
		/// add to avail pool.
		/// </summary>
		/// 
		/// <param name="socket">socket to return</param>
		/// <param name="addToAvail">add to avail pool if true</param>
		public virtual void CheckIn(SockIO socket, bool addToAvail)
		{
			lock (this)
			{
				
				string host = socket.Host;
				if(log.IsDebugEnabled) log.Debug("calling check-in on socket: " + socket.GetHashCode() + " for host: " + host);
				
				// remove from the busy pool
				if(log.IsDebugEnabled) log.Debug("removing socket (" + socket.GetHashCode() + ") from busy pool for host: " + host);
				RemoveSocketFromPool(busyPool, host, socket);
				
				// add to avail pool
				if (addToAvail && socket.Connected)
				{
					if(log.IsDebugEnabled) log.Debug("returning socket (" + socket.GetHashCode() + " to avail pool for host: " + host);
					AddSocketToPool(availPool, host, socket);
				}
			}
		}
		
		/// <summary>
		/// Returns a socket to the avail pool.
		/// 
		/// This is called from SockIO.close().  Calling this method<br/>
		/// directly without closing the SockIO object first<br/>
		/// will cause an IOException to be thrown.
		/// </summary>
		/// 
		/// <param name="socket">socket to return</param>
		public virtual void CheckIn(SockIO socket)
		{
			lock (this)
			{
				CheckIn(socket, true);
			}
		}
		
		/// <summary>
		/// Closes all sockets in the passed in pool.
		/// 
		/// Internal utility method. 
		/// </summary>
		/// 
		/// <param name="pool">pool to close</param>
		private void ClosePool(IDictionary pool)
		{
			foreach(string host in pool.Keys)
			{
				IDictionary sockets = (IDictionary) pool[host];
				
				foreach(SockIO socket in sockets.Keys)
				{
					try
					{
						socket.TrueClose();
					}
					catch(IOException ioe)
					{
						if(log.IsErrorEnabled) log.Error("failed to trueClose socket: " + socket.GetHashCode() + " for host: " + host, ioe);
					}
					
					sockets.Remove(socket);
					//socket = null;
				}
			}
		}
		
		/// <summary>
		/// Shuts down the pool.
		/// 
		/// Cleanly closes all sockets.
		/// Stops the maint thread.
		/// Nulls out all internal maps
		/// </summary>
		public virtual void ShutDown()
		{
			lock (this)
			{
				if(log.IsInfoEnabled) log.Info("SockIOPool shutting down...");
				if (maintThreadRunning)
					StopMaintThread();
				
				if(log.IsInfoEnabled) log.Info("closing all internal pools.");
				ClosePool(availPool);
				ClosePool(busyPool);
				availPool = null;
				busyPool = null;
				buckets = null;
				hostDeadDur = null;
				hostDead = null;
				initialized = false;
				if(log.IsInfoEnabled) log.Info("SockIOPool finished shutting down.");
			}
		}
		
		/// <summary>
		/// Starts the maintenance thread.
		/// 
		/// This thread will manage the size of the active pool<br/>
		/// as well as move any closed, but not checked in sockets<br/>
		/// back to the available pool.
		/// </summary>
		private void StartMaintThread()
		{
			lock (this)
			{
				if (this.maintThreadRunning)
				{
					return ;
				}
				
				MaintThread t = MaintThread.Instance;
				t.Interval = this.maintSleep;
				t.Start();
				this.maintThreadRunning = true;
			}
		}
		
		/// <summary>
		/// Stops the maintenance thread.
		/// </summary>
		private void StopMaintThread()
		{
			lock (this)
			{
				if (!this.maintThreadRunning)
				{
					if(log.IsErrorEnabled) log.Error("maint thread not running, so can't stop it");
					return ;
				}
				
				MaintThread t = MaintThread.Instance;
				t.StopThread();
				this.maintThreadRunning = false;
			}
		}
		
		/// <summary>
		/// Runs self maintenance on all internal pools.
		/// 
		/// This is typically called by the maintenance thread to manage pool size. 
		/// </summary>
		private void SelfMaint()
		{
			lock (this)
			{
				if(log.IsDebugEnabled) log.Debug("Starting self maintenance....");
				
				// go through avail sockets and create/destroy sockets
				// as needed to maintain pool settings
				foreach(string host in availPool.Keys)
				{
					IDictionary sockets = (IDictionary)availPool[host];
					IDictionary bSockets = (IDictionary)busyPool[host];
					
					if(log.IsDebugEnabled)
						log.Debug("Size of avail pool for host (" + host + ") = " + sockets.Count + "\n" +
							"Size of busy pool for host (" + host + ") = " + bSockets.Count);
					
					// if pool is too small (n < minSpare)
					if (sockets.Count < minConn)
					{
						// need to create new sockets
						int need = minConn - sockets.Count;
						if(log.IsDebugEnabled) log.Debug("Need to create " + need + " new sockets for pool for host: " + host);
						
						for (int j = 0; j < need; j++)
						{
							SockIO socket = CreateSocket(host);
							
							if (socket == null)
								break;
							
							AddSocketToPool(availPool, host, socket);
						}
					}
					else if (sockets.Count > maxConn)
					{
						// need to close down some sockets
						int diff = sockets.Count - maxConn;
						int needToClose = (diff <= poolMultiplier)?diff:(diff) / poolMultiplier;
						
						if(log.IsDebugEnabled) log.Debug("Need to remove " + needToClose + " spare sockets for pool for host: " + host);
						
						foreach(SockIO socket in sockets.Keys)
						{
							if (needToClose <= 0)
								break;
							
							// remove stale entries
							long expire = ((long) sockets[socket]);
							
							// if past idle time
							// then close socket
							// and remove from pool
							if ((expire + maxIdle) < (DateTime.Now.Ticks - 621355968000000000) / 10000)
							{
								if(log.IsDebugEnabled) log.Debug("Removing stale entry from pool as it is past its idle timeout and pool is over max spare");
								try
								{
									socket.TrueClose();
								}
								catch(IOException ioe)
								{
									if(log.IsErrorEnabled) log.Error("Failed to close socket", ioe);
								}
								
								sockets.Remove(socket);
								//socket = null;
								needToClose--;
							}
						}
					}
					
					// reset the shift value for creating new SockIO objects
					createShift.Add(host, 0);
				}
				
				if(log.IsDebugEnabled) log.Debug("ending self maintenance.");
			}
		}
		
		/// <summary>
		/// Class which extends thread and handles maintenance of the pool.
		/// </summary>
		private class MaintThread : ThreadClass
		{
			/// <summary>
			/// this is a singleton as we only ever want one thread 
			/// </summary>
			/// 
			/// <returns> MainThread object</returns>
			public new static MaintThread Instance
			{
				get
				{
					lock (typeof(SockIOPool.MaintThread))
					{
						if (thread == null)
						{
							thread = new MaintThread();
						}
						
						return thread;
					}
				}
			}

			virtual public long Interval
			{
				set
				{
					this.interval = value;
				}
				
			}
			
			private long interval = 1000 * 3; // every 3 seconds
			private bool stopThread_Renamed_Field = false;
			
			// single instance of MaintThread
			private static MaintThread thread = null;
			
			internal MaintThread()
			{
				this.IsBackground = true;
			}
			
			/// <summary>
			/// sets stop variable and interupts any wait 
			/// </summary>
			public virtual void StopThread()
			{
				this.stopThread_Renamed_Field = true;
				this.Interrupt();
			}
			
			/// <summary>
			/// Start the thread.
			/// </summary>
			override public void Run()
			{
				while (!this.stopThread_Renamed_Field)
				{
					try
					{
						Thread.Sleep(new TimeSpan((long) 10000 * interval));
						
						// if pool is initialized, then
						// run the maintenance method on itself
						SockIOPool poolObj = SockIOPool.Instance;
						if (poolObj.Initialized)
							poolObj.SelfMaint();
					}
					catch(Exception)
					{
						break;
					}
				}
			}
		}
		
		/// <summary>
		/// MemCached.net client, utility class for Socket IO.
		/// 
		/// This class is a wrapper around a Socket and its streams.
		/// </summary>
		public class SockIO
		{
			private static readonly ILog log = LogManager.GetLogger(typeof(SockIO));

			/// <summary>
			/// Returns the host this socket is connected to 
			/// </summary>
			/// 
			/// <returns> String representation of host (hostname:port)</returns>
			virtual internal string Host
			{
				get
				{
					return this.host;
				}
				
			}
			/// <summary>
			/// checks if the connection is open 
			/// </summary>
			/// 
			/// <returns> true if connected</returns>
			virtual internal bool Connected
			{
				get
				{
					if(sock == null)
						return false;

					// Hacky way to determine if .NET v1.1 TcpClient is connected
					// .NET v2 implements TcpClient.Connected property
					try
					{
						bool test = sock.GetStream().DataAvailable;
					}
					catch(SocketException)
					{
						return false;
					}

					return true;
				}
			}
	
			// data
			private string host;
			private TcpClient sock;
			private BinaryReader inReader;
			private BufferedStream outStream;
			
			/// <summary>
			/// creates a new SockIO object wrapping a socket
			/// connection to host:port, and its input and output streams
			/// </summary>
			/// 
			/// <param name="host">host to connect to</param>
            /// <param name="port">port to connect to</param>
            /// <param name="timeout">int ms to block on data for read</param>
            /// <param name="noDelay">Whether to delay</param>
            /// 
			/// <throws>IOException if an io error occurrs when creating socket</throws>
			/// <throws>UnknownHostException if hostname is invalid </throws>
			internal SockIO(string host, int port, int timeout, bool noDelay)
			{
				sock = new TcpClient(host, port);
				if (timeout >= 0)
					sock.ReceiveTimeout = timeout;
				
				// testing only
				sock.NoDelay = noDelay;
				
				// wrap streams
				inReader = new BinaryReader(sock.GetStream());
				outStream = new BufferedStream(sock.GetStream());
				
				this.host = host + ":" + port;
			}
			
			/// <summary>
			/// Creates a new SockIO object wrapping a socket
			/// connection to host:port, and its input and output streams
			/// </summary>
			/// 
            /// <param name="host">hostname:port</param>
            /// <param name="timeout">Amount of timeout</param>
            /// <param name="noDelay">Whether to delay</param>
            /// 
			/// <throws>IOException if an io error occurrs when creating socket</throws>
			/// <throws>UnknownHostException if hostname is invalid</throws>
			internal SockIO(string host, int timeout, bool noDelay)
			{
				string[] ip = host.Split(':');
				sock = new TcpClient(ip[0], int.Parse(ip[1]));
				if (timeout >= 0)
					sock.ReceiveTimeout = timeout;
				
				// testing only
				sock.NoDelay = noDelay;
				
				// wrap streams
				inReader = new BinaryReader(sock.GetStream());
				outStream = new BufferedStream(sock.GetStream());
				this.host = host;
			}
			
			/// <summary>
			/// Closes socket and all streams connected to it 
			/// </summary>
			/// 
			/// <throws>IOException if fails to close streams or socket</throws>
			internal virtual void TrueClose()
			{
				if(log.IsDebugEnabled) log.Debug("Closing socket for real: " + sock.ToString());
				
				bool err = false;
				StringBuilder errMsg = new StringBuilder();
				
				if (inReader == null || outStream == null || sock == null)
				{
					err = true;
					errMsg.Append("socket or its streams already null in trueClose call");
				}
				
				if (inReader != null)
				{
					try
					{
						inReader.Close();
					}
					catch(IOException ioe)
					{
						if(log.IsErrorEnabled) log.Error("Error closing input stream for socket: " + ToString() + " for host: " + Host, ioe);
						
						errMsg.Append("error closing input stream for socket: " + ToString() + " for host: " + Host + "\n");
						errMsg.Append(ioe.Message);

						err = true;
					}
				}
				
				if (outStream != null)
				{
					try
					{
						outStream.Close();
					}
					catch(IOException ioe)
					{
						if(log.IsErrorEnabled) log.Error("Error closing output stream for socket: " + ToString() + " for host: " + Host, ioe);

						errMsg.Append("Error closing output stream for socket: " + ToString() + " for host: " + Host + "\n");
						errMsg.Append(ioe.Message);

						err = true;
					}
				}
				
				if (sock != null)
				{
					try
					{
						sock.Close();
					}
					catch(IOException ioe)
					{
						if(log.IsErrorEnabled) log.Error("Error closing socket: " + ToString() + " for host: " + Host, ioe);

						errMsg.Append("error closing socket: " + ToString() + " for host: " + Host + "\n");
						errMsg.Append(ioe.Message);

						err = true;
					}
				}
				
				// check in to pool
				if (sock != null)
					SockIOPool.Instance.CheckIn(this, false);
				
				inReader = null;
				outStream = null;
				sock = null;
				
				if (err)
					throw new IOException(errMsg.ToString());
			}
			
			/// <summary>
			/// Sets closed flag and checks in to connection pool
			/// but does not close connections
			/// </summary>
			internal virtual void Close()
			{
				if(log.IsDebugEnabled) log.Debug("Marking socket (" + this.ToString() + ") as closed and available to return to avail pool");
				
				// check in to pool
				SockIOPool.Instance.CheckIn(this);
			}
			
			/// <summary>
			/// Reads a line
			/// intentionally not using the deprecated readLine method from DataInputStream 
			/// </summary>
			/// 
            /// <returns> String that was read in</returns>
            /// 
			/// <throws>IOException if io problems during read</throws>
			internal virtual string ReadLine()
			{
				if (!this.Connected)
				{
					if(log.IsErrorEnabled) log.Error("Attempting to read from closed socket");
					
					throw new IOException("Attempting to read from closed socket");
				}
				
				byte[] b = new byte[1];
				MemoryStream bos = new MemoryStream();
				bool eol = false;
				
				while (SupportClass.ReadInput(inReader.BaseStream, b, 0, 1) != - 1)
				{
					
					if (b[0] == 13)
					{
						eol = true;
					}
					else
					{
						if (eol)
						{
							if (b[0] == 10)
								break;
							
							eol = false;
						}
					}
					
					// cast byte into char array
					bos.Write(b, 0, 1);
				}
				
				if (bos == null || bos.Length <= 0)
				{
					throw new IOException("Stream appears to be dead, so closing it down");
				}
				
				// else return the string
				char[] tmpChar;
				byte[] tmpByte;
				tmpByte = bos.GetBuffer();
				tmpChar = new char[bos.Length];
				Array.Copy(tmpByte, 0, tmpChar, 0, tmpChar.Length);
				return new string(tmpChar).Trim();
			}
			
			/// <summary>
			/// Reads up to end of line and returns nothing 
			/// </summary>
			/// 
			/// <throws>IOException if io problems during read</throws>
			internal virtual void ClearEOL()
			{
				if (!this.Connected)
				{
					if(log.IsErrorEnabled) log.Error("attempting to read from closed socket");
					
					throw new IOException("attempting to read from closed socket");
				}
				
				byte[] b = new byte[1];
				bool eol = false;
				while (SupportClass.ReadInput(inReader.BaseStream, b, 0, 1) != - 1)
				{
					
					// only stop when we see
					// \r (13) followed by \n (10)
					if (b[0] == 13)
					{
						eol = true;
						continue;
					}
					
					if (eol)
					{
						if (b[0] == 10)
							break;
						
						eol = false;
					}
				}
			}
			
			/// <summary>
			/// Reads length bytes into the passed in byte array from dtream
			/// </summary>
			/// 
			/// <param name="b">byte array</param>
			/// 
			/// <throws>IOException if io problems during read </throws>
			internal virtual void Read(byte[] b)
			{
				if (!this.Connected)
				{
					if(log.IsErrorEnabled) log.Error("attempting to read from closed socket");
					
					throw new IOException("attempting to read from closed socket");
				}
				
				int count = 0;
				while (count < b.Length)
				{
					int cnt = SupportClass.ReadInput(inReader.BaseStream, b, count, (b.Length - count));
					count += cnt;
				}
			}
			
			/// <summary>
			/// Flushes output stream 
			/// </summary>
			/// 
			/// <throws>IOException if io problems during read </throws>
			internal virtual void Flush()
			{
				if (!this.Connected)
				{
					if(log.IsErrorEnabled) log.Error("attempting to write to closed socket");
					
					throw new IOException("attempting to write to closed socket");
				}
				outStream.Flush();
			}
			
			/// <summary>
			/// Writes a byte array to the output stream
			/// </summary>
			/// 
			/// <param name="b">byte array to write</param>
			/// 
			/// <throws>IOException if an io error happens</throws>
			internal virtual void Write(byte[] b)
			{
				if (!this.Connected)
				{
					if(log.IsErrorEnabled) log.Error("attempting to write to closed socket");
					
					throw new IOException("attempting to write to closed socket");
				}
				outStream.Write(b, 0, b.Length);
			}
			
			/// <summary>
			/// Use the sockets hashcode for this object
			/// so we can key off of SockIOs 
			/// </summary>
			/// 
			/// <returns>hashcode</returns>
			public override int GetHashCode()
			{
				return (sock == null) ? 0 : sock.GetHashCode();
			}
			
			/// <summary>
			/// Returns the string representation of this socket 
			/// </summary>
			/// <returns>Textual representation</returns>
			public override string ToString()
			{
				return (sock == null)?"":sock.ToString();
			}
		}
	}
}