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
using System.Resources;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Migrator.Framework;

namespace Migrator.Providers.SqlServer
{

    /// <summary>
    /// Migration transformations provider for Microsoft SQL Server.
    /// </summary>
    public interface ISqlServerTransformationProviderViewsExtension : ITransformationProvider
    {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      /// <returns></returns>
      bool ViewExist(string viewName);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      /// <param name="resman"></param>
      void AddView(string viewName, ResourceManager resman);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      /// <param name="viewSelect"></param>
      void AddView(string viewName, string viewSelect);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      /// <param name="fromTableName"></param>
      /// <param name="columns"></param>
      void AddView(string viewName, string fromTableName, params Column[] columns);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      /// <returns></returns>
      string GetViewByName(string viewName);

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      string[] GetViews();

      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      void RemoveView(string viewName);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="viewName"></param>
      void RefreshView(string viewName);
    }

    public interface ISqlServerTransformationProviderFunctionsExtension : ITransformationProvider
    {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="functionName"></param>
      /// <returns></returns>
      bool FunctionExist(string functionName);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="functionCommand"></param>
      void AddFunction(string functionCommand);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="functionName"></param>
      /// <param name="resman"></param>
      void AddFunction(string functionName, ResourceManager resman);

      /// <summary>
      /// 
      /// </summary>
      /// <param name="functionName"></param>
      void RemoveFunction(string functionName);      
    }

    public interface ISqlServerTransformationProviderIndexesExtension : ITransformationProvider
    {

      bool IndexExist(string indexName);


      bool IndexExist(Index index);


      bool IndexExist(string table, Index index);


      bool IndexExist(string table, string indexName);


      bool IndexExist(string table, params Column[] columns);


      void AddIndex(string table, Index index);


      void AddIndex(string table, params SortedColumn[] columns);


      void AddIndex(string table, IndexProperty indexProperty, params SortedColumn[] columns);      


      void RemoveIndex(string table, Index index);


      void RemoveIndex(string table, string indexName);


      void RemoveIndex(string table, params Column[] columns);


      void DisableIndex(string table, Index index);


      void DisableIndex(string table, string indexName);


      void DisableIndex(string table, params Column[] columns);


      void RebuildIndex(string table, Index index);


      void RebuildIndex(string table, string indexName);


      void RebuildIndex(string table, params Column[] columns);


      Index GetIndex(string table, string indexName);


      Index GetIndex(string table, params Column[] columns);


      Index[] GetIndexes(string table);
    }    

    /// <summary>
    /// 
    /// </summary>
    public class IndexInsertionControl : IDisposable
    {
      ISqlServerTransformationProviderIndexesExtension _provider;
      string _table;
      Index _index;
      bool _disposed = false;

      public IndexInsertionControl(ISqlServerTransformationProviderIndexesExtension provider, string table, Index index)
      {
        if (provider == null) throw new ArgumentNullException("provider");
        if (index == null) throw new ArgumentNullException("index");

        _provider = provider;
        _table = table;
        _index = index;
      }

      public IndexInsertionControl(ISqlServerTransformationProviderIndexesExtension provider, string table, Index index, bool postPone) 
      {
        if (provider == null) throw new ArgumentNullException("provider");
        if (index == null) throw new ArgumentNullException("index");

        _provider = provider;
        _table = table;
        _index = index;

        if (!postPone) Disable();
      }

      public void Disable()
      {
        _provider.Logger.Log("Disable '{1}' index on '{0}' table", _table, _index.Name);
        _provider.DisableIndex(_table, _index);
      }

      public void Dispose()
      {
        if (!_disposed)
        {
          _provider.Logger.Log("Rebuild '{1}' index on '{0}' table", _table, _index.Name);
          _provider.RebuildIndex(_table, _index);
          _disposed = true;
        }        
      }

    }

    /// <summary>
    /// Migration transformations provider for Microsoft SQL Server.
    /// </summary>
    public class SqlServerTransformationProvider : 
      TransformationProvider, 
      ISqlServerTransformationProviderViewsExtension, 
      ISqlServerTransformationProviderFunctionsExtension,
      ISqlServerTransformationProviderIndexesExtension
    {
        public SqlServerTransformationProvider(Dialect dialect, string connectionString)
            : base(dialect, connectionString)
        {
            CreateConnection();
        }

    	protected virtual void CreateConnection()
    	{
    		_connection = new SqlConnection();
    		_connection.ConnectionString = _connectionString;
    		_connection.Open();
    	}

        // FIXME: We should look into implementing this with INFORMATION_SCHEMA if possible
        // so that it would be usable by all the SQL Server implementations
    	public override bool ConstraintExists(string table, string name)
        {
            using (IDataReader reader =
                ExecuteQuery(string.Format("SELECT TOP 1 * FROM sysobjects WHERE id = object_id('{0}')", name)))
            {
                return reader.Read();
            }
        }

        public override void AddColumn(string table, string sqlColumn)
        {
            ExecuteNonQuery(string.Format("ALTER TABLE {0} ADD {1}", table, sqlColumn));
        }

		public override bool ColumnExists(string table, string column)
		{
			if (!TableExists(table))
				return false;

			using (IDataReader reader =
				ExecuteQuery(String.Format("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' AND COLUMN_NAME='{1}'", table, column)))
			{
				return reader.Read();
			}
		}

		public override bool TableExists(string table)
		{
            string tableWithoutBrackets = this.RemoveBrackets(table);
            string schemaName = GetSchemaName(tableWithoutBrackets);
            string tableName = this.GetTableName(tableWithoutBrackets);		    
			using (IDataReader reader =
				ExecuteQuery(String.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA ='{0}' AND TABLE_NAME='{1}'", schemaName,tableName)))
			{
				return reader.Read();
			}
		}

        protected string GetSchemaName(string longTableName)
        {
            var splitTable = this.SplitTableName(longTableName);
            return splitTable.Length > 1 ? splitTable[0] : "dbo"; // @FIXME: not DBO by default if we using DSN with user:password to connecting for DB
        }

        protected string[] SplitTableName(string longTableName)
        {            
            return longTableName.Split('.');
            
        }

        protected string GetTableName(string longTableName)
        {
            var splitTable = this.SplitTableName(longTableName);
            return splitTable.Length > 1 ? splitTable[1] : longTableName;
        }

        protected string RemoveBrackets(string stringWithBrackets)
        {
            return stringWithBrackets.Replace("[", "").Replace("]", "");
        }

        public override void RemoveColumn(string table, string column)
        {
            DeleteColumnConstraints(table, column);
            base.RemoveColumn(table, column);
        }
        
        public override void RenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            if (ColumnExists(tableName, newColumnName))
                throw new MigrationException(String.Format("Table '{0}' has column named '{1}' already", tableName, newColumnName));
                
            if (ColumnExists(tableName, oldColumnName)) 
                ExecuteNonQuery(String.Format("EXEC sp_rename '{0}.{1}', '{2}', 'COLUMN'", tableName, oldColumnName, newColumnName));
        }

        public override void RenameTable(string oldName, string newName)
        {
            if (TableExists(newName))
                throw new MigrationException(String.Format("Table with name '{0}' already exists", newName));

            if (TableExists(oldName))
                ExecuteNonQuery(String.Format("EXEC sp_rename {0}, {1}", oldName, newName));
        }

        // Deletes all constraints linked to a column. Sql Server
        // doesn't seems to do this.
        private void DeleteColumnConstraints(string table, string column)
        {
            string sqlContrainte = FindConstraints(table, column);
            List<string> constraints = new List<string>();
            using (IDataReader reader = ExecuteQuery(sqlContrainte))
            {
                while (reader.Read())
                {
                    constraints.Add(reader.GetString(0));
                }
            }
            // Can't share the connection so two phase modif
            foreach (string constraint in constraints)
            {
                RemoveForeignKey(table, constraint);
            }
        }

        // FIXME: We should look into implementing this with INFORMATION_SCHEMA if possible
        // so that it would be usable by all the SQL Server implementations
        protected virtual string FindConstraints(string table, string column)
        {
          return string.Format(
          "SELECT cont.name FROM SYSOBJECTS cont, SYSCOLUMNS col, SYSCONSTRAINTS cnt  "
          + "WHERE cont.parent_obj = col.id AND cnt.constid = cont.id AND cnt.colid=col.colid "
              + "AND col.name = '{1}' AND col.id = object_id('{0}')",
              table, column);
        }

      public override string[] GetTables()
      {
        List<string> tables = new List<string>();
        using (IDataReader reader = ExecuteQuery("SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE != 'VIEW'"))
        {
          while (reader.Read())
            tables.Add((string)reader[0]);
        }
        return tables.ToArray();
      }

      public bool ViewExist(string viewName)
      {
        if (String.IsNullOrEmpty(viewName)) throw new ArgumentNullException(viewName, "viewName");
        string viewWithoutBrackets = this.RemoveBrackets(viewName);
        string schemaName = GetSchemaName(viewWithoutBrackets);
        string viewNameTarget = this.GetTableName(viewWithoutBrackets);
        using (IDataReader reader = ExecuteQuery(String.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{1}' AND TABLE_TYPE='VIEW'", schemaName, viewNameTarget)))
        {
          return reader.Read();
        }
      }

      /// <summary>
      /// Load view content from <paramref name="resman"/> by name <see cref="FORMAT_RESOURCE_VIEW_SELECT"/> ("viewsSelectRule_{0}" template for String.Format method)
      /// </summary>
      /// <param name="viewName">View name</param>
      /// <param name="resman">Resource manager</param>      
      public void AddView(string viewName, ResourceManager resman)
      {
        const string FORMAT_RESOURCE_VIEW_SELECT = "viewsSelectRule_{0}_{1}";
        AddView(viewName, resman.GetString(String.Format(FORMAT_RESOURCE_VIEW_SELECT, viewName, this.CurrentMigration.Version)));
      }

      public void AddView(string viewName, string viewSelect)
      {
        if (ViewExist(viewName))
          throw new MigrationException(String.Format("View with name '{0}' already exists", viewName));

        string viewWithoutBrackets = this.RemoveBrackets(viewName);
        string schemaName = GetSchemaName(viewWithoutBrackets);
        string viewNameTarget = this.GetTableName(viewWithoutBrackets);
        ExecuteNonQuery(String.Format("CREATE VIEW [{0}].{1} AS {2}", schemaName, viewNameTarget, viewSelect));

        if (!ViewExist(viewName))
          throw new MigrationException(String.Format("View with name '{0}' non exists", viewName));
      }

      public void AddView(string viewName, string fromTableName, params Column[] columns)
      {
        if (String.IsNullOrEmpty(viewName)) throw new ArgumentNullException(viewName, "viewName");
        throw new Exception("The method or operation is not implemented.");
      }

      /// <summary>
      /// Static Regex to extract Select part from veiw definition
      /// </summary>
      static readonly System.Text.RegularExpressions.Regex ExcludeCreateViewFromDefinition = new System.Text.RegularExpressions.Regex(
        "^create\\s+view\\s+.+?\\s+as\\s?",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled
      );

      public string GetViewByName(string viewName)
      {
        if (!ViewExist(viewName)) throw new MigrationException(String.Format("View with name '{0}' non exists", viewName));
        string viewWithoutBrackets = this.RemoveBrackets(viewName);
        string schemaName = GetSchemaName(viewWithoutBrackets);
        string viewNameTarget = this.GetTableName(viewWithoutBrackets);
        string viewDefinition = ExecuteScalar(String.Format("SELECT view_definition FROM information_schema.views WHERE TABLE_NAME = '{1}' AND TABLE_SCHEMA = '{0}'", schemaName, viewNameTarget)).ToString();
        
        if (String.IsNullOrEmpty(viewDefinition)) throw new ArgumentNullException(viewName);
        string selectPart = ExcludeCreateViewFromDefinition.Replace(viewDefinition, String.Empty);
        if (String.IsNullOrEmpty(selectPart)) throw new ArgumentException("Cannot match function name from SQL function command", viewName);
        return selectPart;
      }

      public string[] GetViews()
      {
        List<string> views = new List<string>();
        using (IDataReader reader = ExecuteQuery("SELECT table_name as view_name FROM information_schema.views"))
        {
          while (reader.Read())
            views.Add((string)reader[0]);
        }
        return views.ToArray();
      }

      public void RemoveView(string viewName)
      {
        if (!ViewExist(viewName))
          throw new MigrationException(String.Format("View with name '{0}' non exists", viewName));

        string viewWithoutBrackets = this.RemoveBrackets(viewName);
        ExecuteNonQuery(String.Format("DROP VIEW [{0}].{1}", this.GetSchemaName(viewWithoutBrackets), this.GetTableName(viewWithoutBrackets)));

        if (ViewExist(viewName))
          throw new MigrationException(String.Format("View with name '{0}' already exists", viewName));
      }

      public void RefreshView(string viewName)
      {
        if (!ViewExist(viewName))
          throw new MigrationException(String.Format("View with name '{0}' non exists", viewName));

        string viewWithoutBrackets = this.RemoveBrackets(viewName);
        this.ExecuteNonQuery(String.Format("EXEC sp_refreshview '[{0}].{1}'", this.GetSchemaName(viewWithoutBrackets), this.GetTableName(viewWithoutBrackets)));
      }

      /// <summary>
      /// Static Regex to extract long function name (like [dbo].[someFunction])
      /// </summary>
      static readonly System.Text.RegularExpressions.Regex ExcludeFunctionNameFromCommand = new System.Text.RegularExpressions.Regex(
        "^create function ((\\[?[a-z]*?\\]?.)\\[?([a-z]+)\\]?)\\s",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled
      );

      public bool FunctionExist(string functionName)
      {
        if (String.IsNullOrEmpty(functionName)) throw new ArgumentNullException(functionName, "functionName");
        string functionNameWithoutBrackets = this.RemoveBrackets(functionName);
        string schemaName = GetSchemaName(functionNameWithoutBrackets);
        string functionNameTarget = this.GetTableName(functionNameWithoutBrackets);
        using (IDataReader reader = ExecuteQuery(String.Format("SELECT 1 as FuncExist WHERE (SELECT OBJECT_ID (N'[{0}].{1}') as ObjectID) IS NOT NULL", schemaName, functionNameTarget)))
        {
          return reader.Read();
        }
      }      

      public void AddFunction(string functionCommand)
      {
        System.Text.RegularExpressions.Match functionNameMatch = ExcludeFunctionNameFromCommand.Match(functionCommand);
        if (!functionNameMatch.Success) throw new ArgumentException("Cannot match function name from SQL function command", "functionName");

        string functionName = functionNameMatch.Groups[0].Value;
        if (FunctionExist(functionName))
          throw new MigrationException(String.Format("Function with name '{0}' already exists", functionName));

        ExecuteNonQuery(functionCommand);
      }

      public void AddFunction(string functionName, ResourceManager resman)
      {
        if (String.IsNullOrEmpty(functionName)) throw new ArgumentNullException(functionName, "functionName");
        string functionNameWithoutBrackets = this.RemoveBrackets(functionName);
        const string FORMAT_RESOURCE_FUNCTION_COMMAND = "funcSelectRule_{0}_{1}";
        AddFunction(resman.GetString(String.Format(FORMAT_RESOURCE_FUNCTION_COMMAND, functionNameWithoutBrackets, this.CurrentMigration.Version)));
      }

      public void RemoveFunction(string functionName)
      {
        if (!FunctionExist(functionName))
          throw new MigrationException(String.Format("Function with name '{0}' non exists", functionName));

        string functionNameWithoutBrackets = this.RemoveBrackets(functionName);
        ExecuteNonQuery(String.Format("DROP FUNCTION [{0}].{1}", this.GetSchemaName(functionNameWithoutBrackets), this.GetTableName(functionNameWithoutBrackets)));

        if (FunctionExist(functionName))
          throw new MigrationException(String.Format("Function with name '{0}' already exists", functionName));
      }

      string GetIndexName(Index index)
      {
        return GetIndexName("IX", index.Columns as Column[]);
      }

      string GetIndexName(params Column[] columns)
      {
        return GetIndexName("IX", columns);
      }

      string GetIndexName(string indexSuffix, params Column[] columns)
      {
        return String.Concat(indexSuffix, String.Join("_", (new List<Column>(columns)).ConvertAll(delegate(Column column) { return column.Name; }).ToArray()));
      }

      public bool IndexExist(string indexName)
      {
        var exist = SelectScalar("TOP 1 Name", "sysindexes", String.Format("Name = '{0}'", indexName));
        return 1 == ((exist != null) ? Convert.ToInt32(exist) : 0);
      }

      public bool IndexExist(Index index)
      {
        return IndexExist(index.Name);
      }

      public bool IndexExist(string table, string indexName)
      {
        if(!TableExists(table)) throw new TableNotFoundException(table);
        bool exist = false;
        using (IDataReader rdr = ExecuteQuery(String.Format("EXEC sp_helpindex @objname = N'{0}'", table)))
        {
          while (rdr.Read())
          {
            exist = (0 == String.Compare(rdr["index_name"].ToString(), indexName, false));
            if (exist) break;
          }
        }
        return exist;
      }

      public bool IndexExist(string table, params Column[] columns)
      {
        string indexName = GetIndexName(columns);
        return IndexExist(table, indexName);
      }

      public bool IndexExist(string table, Index index)
      {
        return IndexExist(table, index.Name);
      }

      protected string JoinIndexes(IEnumerable<IndexPropertiesMapper> columns)
      {
        List<string> indexes = new List<string>();
        foreach (IndexPropertiesMapper column in columns)
          indexes.Add(column.IndexSql);

        if (indexes.Count == 0)
          return null;

        return String.Join(", ", indexes.ToArray());
      }

      public void AddIndex(string table, Index index)
      {        
        if (null == index) throw new ArgumentNullException("index");
        string indexName = index.Name;
        if (IndexExist(table, indexName)) throw new MigrationException(String.Format("Index '{0}' already exist on table '{1}'", indexName, table));

        string tableWithoutBrackets = this.RemoveBrackets(table);
        string schemaName = GetSchemaName(tableWithoutBrackets);
        string tableName = this.GetTableName(tableWithoutBrackets);

        List<IndexPropertiesMapper> columnProviders = new List<IndexPropertiesMapper>(index.Columns.Length);
        foreach (SortedColumn column in index.Columns) columnProviders.Add(new IndexPropertiesMapper(_dialect, _dialect.GetAndMapColumnProperties(column), column.SortOrder));
        AddIndexInternal(tableName, index, JoinIndexes(columnProviders));
      }

      public void AddIndex(string table, params SortedColumn[] columns)
      {
        string indexName = GetIndexName(columns);
        AddIndex(table, new Index(table, indexName, IndexProperty.None, columns));
      }

      public void AddIndex(string table, IndexProperty indexProperty, params SortedColumn[] columns)
      {
        string indexName = GetIndexName(columns);
        AddIndex(table, new Index(table, indexName, indexProperty, columns));
      }

      void AddIndexInternal(string tableName, Index index, string indexDescription)
      {
        System.Text.StringBuilder prefixIndex = new System.Text.StringBuilder();

        if (index.IsUnique) prefixIndex.Append("UNIQUE ");
        if (index.IsClustered) prefixIndex.Append("CLUSTERED ");
        if (index.IsNonClustered) prefixIndex.Append("NONCLUSTERED ");

        ExecuteNonQuery(String.Format("CREATE {0} INDEX {1} ON {2} ({3})", prefixIndex.ToString(), index.Name, tableName, indexDescription));
      }

      public void RemoveIndex(string table, string indexName)
      {
        if (!IndexExist(table, indexName)) throw new IndexNotFoundException(indexName, table);

        string tableWithoutBrackets = this.RemoveBrackets(table);
        string schemaName = GetSchemaName(tableWithoutBrackets);
        string tableName = this.GetTableName(tableWithoutBrackets);

        ExecuteNonQuery(String.Format("DROP INDEX {0}.{1}", tableName, indexName));        
      }

      public void RemoveIndex(string table, params Column[] columns)
      {
        string indexName = GetIndexName(columns);
        RemoveIndex(table, indexName);
      }

      public void RemoveIndex(string table, Index index)
      {
        if (null == index) throw new ArgumentNullException("index");
        RemoveIndex(table, index.Name);
      }

      public void RebuildIndex(string table, string indexName)
      {
        if (!IndexExist(table, indexName)) throw new IndexNotFoundException(indexName, table);

        string tableWithoutBrackets = this.RemoveBrackets(table);
        string schemaName = GetSchemaName(tableWithoutBrackets);
        string tableName = this.GetTableName(tableWithoutBrackets);	

        ExecuteNonQuery(String.Format("ALTER INDEX {0} ON {1} REBUILD", tableName, indexName));
      }

      public void RebuildIndex(string table, params Column[] columns)
      {
        string indexName = GetIndexName(columns);
        RebuildIndex(table, indexName);
      }

      public void RebuildIndex(string table, Index index)
      {
        if (null == index) throw new ArgumentNullException("index");
        RebuildIndex(table, index.Name);
      }

      class IndexDataHolder
      {
        public string Name { get; set; }
        public string Desc { get; set; }
        public string Keys { get; set; }
        public IndexDataHolder(string indexName, string indexDesc, string indexKeys)
        {
          Name = indexName;
          Desc = indexDesc;
          Keys = indexKeys;
        }
      }

      public Index GetIndex(string table, string indexName)
      {
        if (!IndexExist(table, indexName)) throw new IndexNotFoundException(indexName, table);
        foreach (Index index in GetIndexes(table))
          if (0 == String.Compare(index.Name, indexName, true)) return index;
        throw new IndexNotFoundException(indexName, table);
      }

      public Index GetIndex(string table, params Column[] columns)
      {
        string indexName = GetIndexName(columns);
        return GetIndex(table, indexName);
      }

      public Index[] GetIndexes(string table)
      {
        if (!TableExists(table)) throw new TableNotFoundException(table);    

        string tableWithoutBrackets = this.RemoveBrackets(table);
        string schemaName = GetSchemaName(tableWithoutBrackets);
        string tableName = this.GetTableName(tableWithoutBrackets);

        List<IndexDataHolder> indexHolders = new List<IndexDataHolder>();
        using (IDbCommand commandHelpIndex = GetCommand())
        {
          commandHelpIndex.CommandText = String.Format("EXEC sp_helpindex @objname = N'{1}'", schemaName, tableName);
          using (IDataReader rdr = commandHelpIndex.ExecuteReader())
          {
            while (rdr.Read()) indexHolders.Add(new IndexDataHolder(rdr[0].ToString().Trim(), rdr[1].ToString().Trim(), rdr[2].ToString().Trim()));              
          }          
        }

        return indexHolders.ConvertAll(delegate(IndexDataHolder indexHolder) { return new Index(table, indexHolder.Name, indexHolder.Desc, indexHolder.Keys, this); }).ToArray();
      }

      public void DisableIndex(string table, Index index)
      {
        if (null == index) throw new ArgumentNullException("index");
        DisableIndex(table, index.Name);
      }

      public void DisableIndex(string table, string indexName)
      {
        if (!IndexExist(table, indexName)) throw new IndexNotFoundException(indexName, table);

        string tableWithoutBrackets = this.RemoveBrackets(table);
        string schemaName = GetSchemaName(tableWithoutBrackets);
        string tableName = this.GetTableName(tableWithoutBrackets);

        ExecuteNonQuery(String.Format("ALTER INDEX {0} ON {1} DISABLE", tableName, indexName));
      }

      public void DisableIndex(string table, params Column[] columns)
      {
        string indexName = GetIndexName(columns);
        DisableIndex(table, indexName);
      }
    }
}
