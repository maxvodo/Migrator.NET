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
using System.Data;

namespace Migrator.Framework
{
        /// <summary>
        /// 
        /// </summary>
        public interface IAfterSerializeLoadable
        {
          void SetProvider(ITransformationProvider provider);
        }

	public interface IColumn
	{
		ColumnProperty ColumnProperty { get; set; }

		string Name { get; set; }

		DbType Type { get; set; }

		int Size { get; set; }

		bool IsIdentity { get; }

		bool IsPrimaryKey { get; }

		object DefaultValue { get; set; }
	}  

        [Flags]
        public enum DataCompression
        {
          None = 0,

          Row = 1,

          Page = 2
        }

        public interface IRelationalOptions
        {
          bool PAD_INDEX { get; set; }
          int FILLFACTOR { get; set; }
          bool SORT_IN_TEMPDB { get; set; }
          bool IGNORE_DUP_KEY { get; set; }
          bool STATISTICS_NORECOMPUTE { get; set; }
          bool STATISTICS_INCREMENTAL { get; set; }
          bool DROP_EXISTING { get; set; }
          bool ONLINE { get; set; }    
          bool DATA_COMPRESSION { get; set; }
        }

        public interface ISortedColumn : IColumn
        {
          ColumnSortOrderProperty SortOrder { get; set; }
        }

        public interface IIndex : IAfterSerializeLoadable
        {
          IndexProperty IndexProperty { get; set; }

          string Table { get; }

          string Name { get; set; }

          bool IsClustered { get; }

          bool IsNonClustered { get; }

          bool IsUnique { get; }

          ISortedColumn[] Columns { get; }

          IRelationalOptions RelationalOption { get; }

          bool IgnoreDuplicateKey { set; get; }

          bool DropExistingIndex { set; get; }
        }
}