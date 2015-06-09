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
using System.Xml.Serialization;

namespace Migrator.Framework
{
    /// <summary>
    /// Represents a table column.
    /// </summary>
    public class Column : IColumn
    {
        private string _name;
        private DbType _type;
        private int _size;
        private ColumnProperty _property;
        private object _defaultValue;

		public Column(string name)
		{
			Name = name;
		}

    	public Column(string name, DbType type)
        {
            Name = name;
            Type = type;
        }

        public Column(string name, DbType type, int size)
        {
            Name = name;
            Type = type;
            Size = size;
        }

		public Column(string name, DbType type, object defaultValue)
		{
			Name = name;
			Type = type;
			DefaultValue = defaultValue;
		}

    	public Column(string name, DbType type, ColumnProperty property)
        {
            Name = name;
            Type = type;
            ColumnProperty = property;
        }

        public Column(string name, DbType type, int size, ColumnProperty property)
        {
            Name = name;
            Type = type;
            Size = size;
            ColumnProperty = property;
        }

        public Column(string name, DbType type, int size, ColumnProperty property, object defaultValue)
        {
            Name = name;
            Type = type;
            Size = size;
            ColumnProperty = property;
            DefaultValue = defaultValue;
        }

        public Column(string name, DbType type, ColumnProperty property, object defaultValue)
        {
            Name = name;
            Type = type;
            ColumnProperty = property;
            DefaultValue = defaultValue;
        }

        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }

        public DbType Type
        {
            get { return _type; }
            set { _type = value; }
        }

        public int Size
        {
            get { return _size; }
            set { _size = value; }
        }

        public ColumnProperty ColumnProperty
        {
            get { return _property; }
            set { _property = value; }
        }

        public object DefaultValue
        {
            get { return _defaultValue; }
            set { _defaultValue = value; }
        }
        
        public bool IsIdentity 
        {
            get { return (ColumnProperty & ColumnProperty.Identity) == ColumnProperty.Identity; }
        }
        
        public bool IsPrimaryKey 
        {
            get { return (ColumnProperty & ColumnProperty.PrimaryKey) == ColumnProperty.PrimaryKey; }
        }
    }

    public class SortedColumn : Column, ISortedColumn
    {

      public SortedColumn(Column column, ColumnSortOrderProperty sortOrder)
        : base(column.Name, column.Type, column.ColumnProperty, column.DefaultValue)
		  {
        SortOrder = sortOrder;
		  }    	

      #region ISortedColumn Members

      public ColumnSortOrderProperty SortOrder
      {
        get;
        set;
      }

      #endregion
    }

    public class Index : IIndex, IXmlSerializable
    {
      public Index()
      {
  
      }

      public Index(string table, string name, IndexProperty property, params SortedColumn[] columns)
      {
        this.Table = table;
        this.Name = name;
        this.IndexProperty = property;
        this.Columns = columns;
      }

      public Index(string table, string name, string desc, string keys, ITransformationProvider provider)
      {
        if(null == provider) throw new ArgumentNullException("provider");
        this.Table = table;
        this.Name = name;
        this.Provider = provider;
        this.IndexDescription = desc;
        this.IndexKeys = keys;
        this.LoadRelationalOptions(provider);
        this.Provider = null;        
      }

      void LoadKeysFromProvider(ITransformationProvider provider)
      {
        if(null == provider) throw new ArgumentNullException("provider");
        Provider = provider;
        this.IndexKeys = String.Concat(String.Empty, this.IndexKeys);
        Provider = null;
      }

      #region IIndex Members
      
      public IndexProperty IndexProperty
      {
        get; 
        set;
      }
      
      ITransformationProvider Provider
      {
        get;
        set;
      }
      
      public string Table { 
        get;
        protected set;
      }
      
      public string Name
      {
        get;
        set;
      }
      
      public bool IsClustered
      {
        get { return (IndexProperty & IndexProperty.Clustered) == IndexProperty.Clustered; }
      }
      
      public bool IsNonClustered
      {
        get { return (IndexProperty & IndexProperty.NonClustered) == IndexProperty.NonClustered; }
      }
      
      public bool IsUnique
      {
        get { return (IndexProperty & IndexProperty.Unique) == IndexProperty.Unique; }
      }
      
      public bool IsPrimaryKey
      {
        get { return (IndexProperty & IndexProperty.PrimaryKey) == IndexProperty.PrimaryKey; }
      }
      
      public ISortedColumn[] Columns
      {
        get;
        protected set;
      }
      
      public IRelationalOptions RelationalOption
      {
        get { throw new System.Exception("The method or operation is not implemented."); }
        protected set { throw new System.Exception("The method or operation is not implemented."); }
      }
      
      public bool IgnoreDuplicateKey
      {
        get;
        set;
      }
      
      public bool DropExistingIndex
      {
        get;
        set;
      }            

      #endregion

      void LoadRelationalOptions(ITransformationProvider provider)
      {
        // @TODO: impl
      }

      static readonly System.Text.RegularExpressions.Regex RX_DESC = new System.Text.RegularExpressions.Regex(
        "(nonclustered)|(clustered)|(unique)|(primary key)|(located on)|(stats no recompute)", 
        System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase
      );

      protected string IndexDescription
      {
        get
        {
          System.Text.StringBuilder retDesc = new System.Text.StringBuilder();
          string CommaSpace = ", ";

          if (IsNonClustered) retDesc.Append(CommaSpace).Append("nonclustered");
          if (IsClustered) retDesc.Append(CommaSpace).Append("clustered");
          if (IsUnique) retDesc.Append(CommaSpace).Append("unique");
          if (IsPrimaryKey) retDesc.Append(CommaSpace).Append("primary key");

          retDesc.Append(" located on PRIMARY"); // @WARNING: clarify

          string ret = retDesc.ToString();
          return (0 == ret.IndexOf(CommaSpace)) ? ret.Remove(0, CommaSpace.Length) : ret;
        }
        private set
        {
          foreach (System.Text.RegularExpressions.Match match in RX_DESC.Matches(value))
          {
            switch (match.Value.Trim().ToLower())
            {
              case "nonclustered":
                IndexProperty |= IndexProperty.NonClustered;
                break;
              case "clustered":
                IndexProperty |= IndexProperty.Clustered;
                break;
              case "unique":
                IndexProperty |= IndexProperty.Unique;
                break;
              case "primary key":
                IndexProperty |= IndexProperty.PrimaryKey;
                break;
              case "located on":
                break;
              case "stats no recompute":
                break;

              default:
                break;
            }
          }
        }
      }

      static readonly System.Text.RegularExpressions.Regex RX_KEYS = new System.Text.RegularExpressions.Regex("^(?<column>\\w+)(?:\\((?<sort>[\\-\\+])\\))?$", System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.ExplicitCapture);

      private string _IndexKeys;
      protected string IndexKeys
      {
        get
        {
          System.Text.StringBuilder retKeys = new System.Text.StringBuilder();
          string CommaSpace = ", ";
          foreach (SortedColumn column in this.Columns)
            retKeys.Append(CommaSpace).Append(column.Name).Append(column.SortOrder == ColumnSortOrderProperty.Default ? String.Empty : "(-)");

          string ret = retKeys.ToString();
          return (0 == ret.IndexOf(CommaSpace)) ? ret.Remove(0, CommaSpace.Length) : ret;
        }
        private set
        {
          this.Columns = new ISortedColumn[0];
          if (null == this.Provider)
          {
            this._IndexKeys = value;
            return;
          }

          System.Collections.Generic.Dictionary<string, Column> cacheColumns = new System.Collections.Generic.Dictionary<string, Column>();
          foreach (Column column in this.Provider.GetColumns(Table)) cacheColumns.Add(column.Name, column);

          System.Collections.Generic.List<SortedColumn> columns = new System.Collections.Generic.List<SortedColumn>();                    
          foreach (string part in ((String.IsNullOrEmpty(value) && !String.IsNullOrEmpty(_IndexKeys)) ? this._IndexKeys : value).Split(new char[] { ',' }))
          {
            string trimmedPart = part.Trim();
            foreach (System.Text.RegularExpressions.Match match in RX_KEYS.Matches(trimmedPart))
            {
              string columnName = match.Groups["column"].Value;
              ColumnSortOrderProperty columnSortOrder = (0 == String.Compare("-", match.Groups["sort"].Value)) ? ColumnSortOrderProperty.Desc : ColumnSortOrderProperty.Default;
              
              if(!cacheColumns.ContainsKey(columnName)) throw new ColumnNotFoundException(columnName, Table);
              columns.Add(new SortedColumn(cacheColumns[columnName], columnSortOrder));
            }
          }

          this._IndexKeys = value;
          this.Columns = columns.ToArray();
        }
      }

      public void SetProvider(ITransformationProvider provider)
      {
        LoadKeysFromProvider(provider);
      }

      #region IXmlSerializable Members

      public System.Xml.Schema.XmlSchema GetSchema()
      {
        return new System.Xml.Schema.XmlSchema();
      }

      public void ReadXml(System.Xml.XmlReader reader)
      {
        reader.MoveToContent();
        this.Name = reader.GetAttribute("Name");
        this.Table = reader.GetAttribute("Table");        
        reader.ReadStartElement();
        this.IndexDescription = reader.ReadElementString("IndexDescription");
        this.IndexKeys = reader.ReadElementString("IndexKeys");
        reader.ReadEndElement();

        if (String.IsNullOrEmpty(this.Name)) throw new System.Xml.XmlException("ReadXml", new ArgumentNullException("Name"));
        if (String.IsNullOrEmpty(this.Table)) throw new System.Xml.XmlException("ReadXml", new ArgumentNullException("Table"));
      }

      public void WriteXml(System.Xml.XmlWriter writer)
      {
        writer.WriteAttributeString("Name", Name);
        writer.WriteAttributeString("Table", Table);
        writer.WriteElementString("IndexDescription", IndexDescription);
        writer.WriteElementString("IndexKeys", IndexKeys);
      }      
      #endregion
    }
}
