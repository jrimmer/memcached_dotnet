//
// MemCached .NET Client Utilities
// 
// Copyright (c) 2004 Jason Rimmer <jrimmer@irth.net>
// All rights reserved.
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
using System.Reflection;
using System.Text;
using System.Threading;
using System.IO;

namespace red5.memcached
{
    /// <summary>
    /// Contains conversion support elements such as classes, interfaces and static methods.
    /// </summary>
    public sealed class SupportClass
    {
        /// <summary>
        /// Reads a number of characters from the current source Stream and writes the data to the target array at the specified index.
        /// </summary>
        /// <param name="sourceStream">The source Stream to read from.</param>
        /// <param name="target">Contains the array of characteres read from the source Stream.</param>
        /// <param name="start">The starting index of the target array.</param>
        /// <param name="count">The maximum number of characters to read from the source Stream.</param>
        /// <returns>The number of characters read. The number will be less than or equal to count depending on the data available in the source Stream. Returns -1 if the end of the stream is reached.</returns>
        public static Int32 ReadInput(Stream sourceStream, byte[] target, int start, int count)
        {
            // Returns 0 bytes if not enough space in target
            if(target.Length == 0)
                return 0;

            byte[] receiver = new byte[target.Length];
            int bytesRead = sourceStream.Read(receiver, start, count);

            // Returns -1 if EOF
            if(bytesRead == 0)
                return -1;

            for(int i = start;i < start + bytesRead;i++)
                target[i] = (byte)receiver[i];

            return bytesRead;
        }

		/// <summary>
		/// Extract function sigature as string
		/// </summary>
		/// <param name="method">Method whose signature you want to reprot</param>
		/// <returns>Reported signature</returns>
		public static string DescribeFunctionSignature(MethodBase method)
		{
			StringBuilder results = new StringBuilder();

			results.Append(method.Name);
        
			ParameterInfo[] parms = method.GetParameters();

			results.Append("(");
			for(int count = 0; count < parms.Length; count++)
			{
				results.Append(parms[count].ParameterType.ToString());               
				if( (count + 1) < parms.Length)
				{
					results.Append(",");
				}
			}
			results.Append(")");

			return results.ToString();
		}
	} 

    /// <summary>
    /// This interface should be implemented by any class whose instances are intended 
    /// to be executed by a thread.
    /// </summary>
    public interface IThreadRunnable
    {
        /// <summary>
        /// This method has to be implemented in order that starting of the thread causes the object's 
        /// run method to be called in that separately executing thread.
        /// </summary>
        void Run();
    }

    /// <summary>
    /// Support class used to handle threads
    /// </summary>
    public class ThreadClass : IThreadRunnable
    {
        /// <summary>
        /// The instance of Thread
        /// </summary>
        private Thread threadField;

        /// <summary>
        /// Initializes a new instance of the ThreadClass class
        /// </summary>
        public ThreadClass()
        {
            threadField = new Thread(new ThreadStart(Run));
        }

        /// <summary>
        /// Initializes a new instance of the Thread class.
        /// </summary>
        /// <param name="Name">The name of the thread</param>
        public ThreadClass(string Name)
        {
            threadField = new Thread(new ThreadStart(Run));
            this.Name = Name;
        }

        /// <summary>
        /// Initializes a new instance of the Thread class.
        /// </summary>
        /// <param name="Start">A ThreadStart delegate that references the methods to be invoked when this thread begins executing</param>
        public ThreadClass(ThreadStart Start)
        {
            threadField = new Thread(Start);
        }

        /// <summary>
        /// Initializes a new instance of the Thread class.
        /// </summary>
        /// <param name="Start">A ThreadStart delegate that references the methods to be invoked when this thread begins executing</param>
        /// <param name="Name">The name of the thread</param>
        public ThreadClass(ThreadStart Start, string Name)
        {
            threadField = new Thread(Start);
            this.Name = Name;
        }

        /// <summary>
        /// This method has no functionality unless the method is overridden
        /// </summary>
        public virtual void Run()
        {
        }

        /// <summary>
        /// Causes the operating system to change the state of the current thread instance to ThreadState.Running
        /// </summary>
        public virtual void Start()
        {
            threadField.Start();
        }

        /// <summary>
        /// Interrupts a thread that is in the WaitSleepJoin thread state
        /// </summary>
        public virtual void Interrupt()
        {
            threadField.Interrupt();
        }

        /// <summary>
        /// Gets the current thread instance
        /// </summary>
        public Thread Instance
        {
            get
            {
                return threadField;
            }
            set
            {
                threadField = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the thread
        /// </summary>
        public string Name
        {
            get
            {
                return threadField.Name;
            }
            set
            {
                if(threadField.Name == null)
                    threadField.Name = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating the scheduling priority of a thread
        /// </summary>
        public ThreadPriority Priority
        {
            get
            {
                return threadField.Priority;
            }
            set
            {
                threadField.Priority = value;
            }
        }

        /// <summary>
        /// Gets a value indicating the execution status of the current thread
        /// </summary>
        public bool IsAlive
        {
            get
            {
                return threadField.IsAlive;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether or not a thread is a background thread.
        /// </summary>
        public bool IsBackground
        {
            get
            {
                return threadField.IsBackground;
            }
            set
            {
                threadField.IsBackground = value;
            }
        }

        /// <summary>
        /// Blocks the calling thread until a thread terminates
        /// </summary>
        public void Join()
        {
            threadField.Join();
        }

        /// <summary>
        /// Blocks the calling thread until a thread terminates or the specified time elapses
        /// </summary>
        /// <param name="MiliSeconds">Time of wait in milliseconds</param>
        public void Join(long MiliSeconds)
        {
            lock(this)
            {
                threadField.Join(new TimeSpan(MiliSeconds * 10000));
            }
        }

        /// <summary>
        /// Blocks the calling thread until a thread terminates or the specified time elapses
        /// </summary>
        /// <param name="MiliSeconds">Time of wait in milliseconds</param>
        /// <param name="NanoSeconds">Time of wait in nanoseconds</param>
        public void Join(long MiliSeconds, int NanoSeconds)
        {
            lock(this)
            {
                threadField.Join(new TimeSpan(MiliSeconds * 10000 + NanoSeconds * 100));
            }
        }

        /// <summary>
        /// Gets the currently running thread
        /// </summary>
        /// <returns>The currently running thread</returns>
        public static ThreadClass Current()
        {
            ThreadClass CurrentThread = new ThreadClass();
            CurrentThread.Instance = Thread.CurrentThread;
            return CurrentThread;
        }

        /// <summary>
        /// Obtain a String that represents the current Object
        /// </summary>
        /// <returns>A String that represents the current Object</returns>
        public override string ToString()
        {
            return "Thread[" + Name + "," + Priority.ToString() + "," + "" + "]";
        }
    }
}
