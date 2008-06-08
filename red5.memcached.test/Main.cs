/// <summary>
/// MemCachedTest.java
/// Test class for testing memcached java client.
/// 
/// Copyright (c) 2004 Jason Rimmer <jrimmer@irth.net> and 
/// Greg Whalin <greg@meetup.com>
/// All rights reserved.
/// 
/// Ported from Greg Whalin's <greg@meetup.com> java client
/// 
/// See the memcached website:
/// http://www.danga.com/memcached/
/// 
/// This library is free software; you can redistribute it and/or
/// modify it under the terms of the GNU Lesser General Public
/// License as published by the Free Software Foundation; either
/// version 2.1 of the License, or (at your option) any later
/// version.
/// 
/// This library is distributed in the hope that it will be
/// useful, but WITHOUT ANY WARRANTY; without even the implied
/// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
/// PURPOSE.  See the GNU Lesser General Public License for more
/// details.
/// 
/// You should have received a copy of the GNU Lesser General Public
/// License along with this library; if not, write to the Free Software
/// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
/// </summary>

using System;
using System.Collections;
using System.Text;
using System.Threading;

using red5.memcached;

namespace red5.memcached.test
{
    /// <summary>
    /// Test memcached client
    /// </summary>
	public class MemCachedTest
    {
        // store results from threads
        private static Hashtable threadInfo = Hashtable.Synchronized(new Hashtable());

        /// <summary> This runs through some simple tests of the MemCacheClient.
        /// 
        /// Command line args:
        /// args[0] = number of threads to spawn
        /// args[1] = number of runs per thread
        /// args[2] = size of object to store 
        /// 
        /// </summary>
        /// <param name="args">the command line arguments
        /// </param>
        [STAThread]
        public static void Main(string[] args)
        {
            string[] serverlist = new string[] {
                "192.168.1.108:11211"
            };

            // initialize the pool for memcache servers
            SockIOPool pool = SockIOPool.Instance;
            pool.Servers = serverlist;

            pool.InitConn = 5;
            pool.MinConn = 5;
            pool.MaxConn = 50;
            pool.MaintSleep = 30;

            pool.Nagle = false;
            pool.Initialize();

            int threads = Int32.Parse(args[0]);
            int runs = Int32.Parse(args[1]);
            int size = 1024 * Int32.Parse(args[2]); // how many kilobytes

            // get object to store
            int[] obj = new int[size];
            for(int i = 0;i < size;i++)
            {
                obj[i] = i;
            }

            string[] keys = new string[size];
            for(int i = 0;i < size;i++)
            {
                keys[i] = "test_key" + i;
            }

            for(int i = 0;i < threads;i++)
            {
                bench b = new bench(runs, i, obj, keys);
                b.Start();
            }

            int i2 = 0;
            while(i2 < threads)
            {
                if(threadInfo.ContainsKey((Int32)i2))
                {
                    System.Console.Out.WriteLine(((StringBuilder)threadInfo[(Int32)i2]));
                    i2++;
                }
                else
                {
                    try
                    {
                        ThreadClass.Current();
                        Thread.Sleep(new TimeSpan((System.Int64)10000 * 1000));
                    }
                    catch(ThreadInterruptedException e)
                    {
                        Console.Error.Write(e.StackTrace);
                        Console.Error.Flush();
                    }
                }
            }

            pool.ShutDown();
			
			System.Environment.Exit(1);
        }

        /// <summary> Test code per thread. </summary>
        private class bench : ThreadClass
        {
            private int runs;
            private int threadNum;
            private int[] storeObj;
            private string[] keys;
            private int size;

            public bench(int runs, int threadNum, int[] storeObj, string[] keys)
            {
                this.runs = runs;
                this.threadNum = threadNum;
                this.storeObj = storeObj;
                this.keys = keys;
                this.size = storeObj.Length;
            }

            override public void Run()
            {

                StringBuilder result = new StringBuilder();

                // get client instance
                MemCachedClient mc = new MemCachedClient();
                mc.CompressEnable = false;
                mc.CompressThreshold = 0;
                mc.Serialize = true;

				// timing vars
				DateTime start;
				TimeSpan elapse;
				float avg;

                // time stores
                start = DateTime.Now;
                for(int i = 0;i < runs;i++)
                {
                    mc.Set(keys[i], storeObj);
                }
                elapse = DateTime.Now - start;
                avg = (float)(elapse.Milliseconds) / runs;
                result.Append("\nthread " + threadNum + ": runs: " + runs + " stores of obj " + (size / 1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse.Milliseconds + " ms)");

                // time gets
                start = DateTime.Now;
                for(int i = 0;i < runs;i++)
                {
                    mc.Get(keys[i]);
                }
				elapse = DateTime.Now - start;
				avg = (float)(elapse.Milliseconds) / runs;
				result.Append("\nthread " + threadNum + ": runs: " + runs + " gets of obj " + (size / 1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse.Milliseconds + " ms)");

                // time deletes
                start = DateTime.Now;
                for(int i = 0;i < runs;i++)
                {
                    mc.Delete(keys[i]);
                }
				elapse = DateTime.Now - start;
				avg = (float)(elapse.Milliseconds) / runs;
				result.Append("\nthread " + threadNum + ": runs: " + runs + " deletes of obj " + (size / 1024) + "KB -- avg time per req " + avg + " ms (total: " + elapse.Milliseconds + " ms)");

                MemCachedTest.threadInfo[(Int32)threadNum] = result;
            }
        }
    }
}