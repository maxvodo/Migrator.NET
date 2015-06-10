# Migrator.NET
Database migrations for .NET. Based on the idea of Rails ActiveRecord Migrations.

Below some features for Migrator.NET and SqlServer provider:
- Persistent store in SchemaInfo table
- Views, Functions, Indexes support

Services adapter class for using new interfaces (like a `ISqlServerTransformationProviderIndexesExtension`):
```c#
/// <summary>
/// Generic adapter for provide Database member with <typeparam name="T">T</typeparam> type
/// </summary>
internal class SqlDatabaseWrapper<T> : IDisposable where T : class
{
  public T Database { get; private set; }

  public SqlDatabaseWrapper(ITransformationProvider database)
  {
    this.Database = database as T;
    if (null == this.Database) throw new MigrationException(new ArgumentNullException("SqlDatabase").Message);
  }

  public void Dispose() { }
}

/// <summary>
/// Just test migration
/// </summary>
[Migration(00000101)]
public sealed class TestOne : Migration
{
  public override void Up()
  {
    // Wrap this.Database object to generic SqlDatabaseWrapper<T> wrapper for requested interface type
    using (var SqlDatabase = new SqlDatabaseWrapper<ISqlServerTransformationProviderIndexesExtension>(this.Database))
    {
      this.Database.AddColumn("someTable", new Column("ID", DbType.Int32, ColumnProperty.NotNull, 0));
      this.Store.Set("someIndex", SqlDatabase.Database.GetIndex("someTable", "someIndex"));
      SqlDatabase.Database.RemoveIndex("someTable", "someIndex");
    }
  }
}
```


#### Storing additional information in SchemaInfo table
Interface `ITransformationProviderStore` provides methods to store and read data (with simple and serializable types):
```c#
[Serializable]
public class SomeClass
{
  public SomeClass() { }
  public SomeClass(string name) { this.Name = name; }
  string Name { get; set; }
}

//
// Or maybe you want using interface IXmlSerializable 
// to provide manual serialize/deserialize procedure

/// <summary>
/// 
/// </summary>
[Migration(00000102)]
public sealed class TestTwo : Migration
{
  public override void Up()
  {
    this.Store.Set("yep", true);
    this.Store.Set("1", 1);

    var sc = new SomeClass("me");
    this.Store.Set(sc.Name, sc);
  }

  public override void Down()
  {
    var t1 = this.Store.Get<bool>("yep");
    var t2 = this.Store.Get<int>("1");
    var t3 = this.Store.Get<int>("3", 3);
    var sc = this.Store.Get<SomeClass>("me");
  }
}
```


#### Views support for SqlServer provider
Views support present `ISqlServerTransformationProviderViewsExtension` interface:
```c#
// ...
using (var SqlDatabase = new SqlDatabaseWrapper<ISqlServerTransformationProviderViewsExtension>(this.Database))
{
  if (!SqlDatabase.Database.ViewExist("someView")) throw new ViewNotFoundException("someView");
}
// ...
```


#### Functions support SqlServer provider
Functions support present `ISqlServerTransformationProviderFunctionsExtension` interface:
```c#
// ...
using (var SqlDatabase = new SqlDatabaseWrapper<ISqlServerTransformationProviderFunctionsExtension>(this.Database))
{
  if (!SqlDatabase.Database.FunctionExist("someFunc")) throw new MigrationException("someFunc");
}
// ...
```


#### Indexes support for SqlServer provider
Indexes support present `ISqlServerTransformationProviderIndexesExtension` interface:
```c#
// ...
using (var SqlDatabase = new SqlDatabaseWrapper<ISqlServerTransformationProviderIndexesExtension>(this.Database))
{
  if (!SqlDatabase.Database.IndexExist("someTable", "someIndex")) throw new IndexNotFoundException("someIndex", "someTable");
}
// ...
```
