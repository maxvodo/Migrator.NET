namespace Migrator.Framework
{
    public interface IMigration
    {
        string Name { get; }

        /// <summary>
        /// Represents the database.
        /// <see cref="ITransformationProvider"></see>.
        long Version { get; }

        /// <summary>
        /// Version of previous migration
        /// </summary>
        long Previous { get; set; }

        /// <summary>
        /// Represents the database.
        /// <see cref="ITransformationProvider"></see>.
        /// </summary>
        /// <seealso cref="ITransformationProvider">Migration.Framework.ITransformationProvider</seealso>
        ITransformationProvider Database { get; set; }

        /// <summary>
        /// Defines tranformations to port the database to the current version.
        /// </summary>
        void Up();

        /// <summary>
        /// This is run after the Up transaction has been committed
        /// </summary>
        void AfterUp();

        /// <summary>
        /// Defines transformations to revert things done in <c>Up</c>.
        /// </summary>
        void Down();

        /// <summary>
        /// This is run after the Down transaction has been committed
        /// </summary>
        void AfterDown();

        /// <summary>
        /// This gets called once on the first migration object.
        /// </summary>
        void InitializeOnce(string[] args);

        /// <summary>
        /// Using for store and share some user data between migrations
        /// </summary>
        ITransformationProviderStore Store { get; set; }
    }
}