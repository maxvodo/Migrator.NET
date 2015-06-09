#region License
//The contents of this file are subject to the Mozilla Public License
//Version 1.1 (the "License"); you may not use this file except in
//compliance with the License. You may obtain a copy of the License at
//http://www.mozilla.org/MPL/
//Software distributed under the License is distributed on an "AS IS"
//basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
//License for the specific language governing rights and limitations
//under the License.
#endregion

using System;

namespace Migrator.Framework
{
	/// <summary>
	/// Base class for migration errors.
	/// </summary>
	public class MigrationException : Exception
	{
	    public MigrationException(string message)
			: base(message) {}
			
		public MigrationException(string message, Exception cause)
			: base(message, cause) {}
			
		public MigrationException(string migration, int version, Exception innerException)
			: base(String.Format("Exception in migration {0} (#{1})", migration, version), innerException) {}
	}

        /// <summary>
        /// Table not found exception.
        /// </summary>
        public sealed class TableNotFoundException : MigrationException
        {
          public TableNotFoundException(string table) : base(string.Format("Table '{0}' doesn't exist", table)) { }
        }

        /// <summary>
        /// Column not found exception.
        /// </summary>
        public sealed class ColumnNotFoundException : MigrationException
        {
          public ColumnNotFoundException(string column, string table) : base(string.Format("Column '{0}' doesn't exist in '{1}' table", column, table)) { }
        }

        /// <summary>
        /// View not found exception.
        /// </summary>
        public sealed class ViewNotFoundException : MigrationException
        {
          public ViewNotFoundException(string view) : base(string.Format("View '{0}' doesn't exist", view)) { }
        }

        /// <summary>
        /// View not found exception.
        /// </summary>
        public sealed class IndexNotFoundException : MigrationException
        {
          public IndexNotFoundException(string index) : base(string.Format("Index '{0}' doesn't exist", index)) { }
          public IndexNotFoundException(string index, string table) : base(string.Format("Index '{0}' doesn't exist in '{1}' table", index, table)) { }
        }
      }
