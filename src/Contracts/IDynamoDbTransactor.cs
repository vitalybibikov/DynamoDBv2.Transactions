using System.Linq.Expressions;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Contracts
{
    /// <summary>
    /// Main interface for DynamoDB transactions.
    /// </summary>
    public interface IDynamoDbTransactor : IAsyncDisposable
    {
        /// <summary>
        ///  Gets a value indicating whether an error has occurred during the execution of the transaction.
        /// </summary>
        public bool ErrorDuringExecution { get; }

        /// <summary>
        /// Initiates an operation to create or update an item, that will be part of a transaction.
        /// </summary>
        /// <typeparam name="T">Any table item to save</typeparam>
        /// <param name="item">Item to create or update</param>
        public void CreateOrUpdate<T>(T item);

        /// <summary>
        /// Initiates an operation to patch a single property of an item, that will be part of a transaction.
        /// </summary>
        /// <typeparam name="T">Any table item to patch</typeparam>
        /// <param name="model">A model that contains a property that should be patched.</param>
        /// <param name="propertyName">Name of the property that should be patched.</param>
        public void PatchAsync<T>(T model, string propertyName);

        /// <summary>
        /// Initiates an operation to patch a single property of an item, that will be part of a transaction
        /// </summary>
        /// <typeparam name="TModel">A model that contains a property that should be patched.</typeparam>
        /// <typeparam name="TValue">Value of that model that should be patched</typeparam>
        /// <param name="keyValue">HASH key of the item in the operation</param>
        /// <param name="propertyExpression">Property expression to patch a property in the model.</param>
        /// <param name="value">Value to be set.</param>
        public void PatchAsync<TModel, TValue>(string keyValue, Expression<Func<TModel, TValue?>> propertyExpression, TValue value);

        /// <summary>
        /// Initiates an operation to delete an item, that will be part of a transaction.
        /// </summary>
        /// <typeparam name="T">Any table item to save</typeparam>
        /// <param name="key">Name of the HASH key on the table</param>
        /// <param name="deletedItemValue">Value of the HASH Key</param>
        public void DeleteAsync<T>(string key, string deletedItemValue);

        /// <summary>
        /// Initiates an operation to delete an item, that will be part of a transaction.
        /// </summary>
        /// <typeparam name="TModel">A model that contains a property that should be patched.</typeparam>
        /// <typeparam name="TKeyValue">Hash key value of that model that should be deleted.</typeparam>
        /// <param name="propertyNameExpression">Property expression to define name of the HASH property of the table.</param>
        /// <param name="deletedItemValue">Value to delete.</param>
        public void DeleteAsync<TModel, TKeyValue>(Expression<Func<TModel, string>> propertyNameExpression, string deletedItemValue);

        /// <summary>
        /// Adds a raw request of type <see cref="ITransactionRequest"/> to the transaction.
        /// </summary>
        /// <param name="request">Raw request operation to DynamoDB </param>
        public void AddRawRequest(ITransactionRequest request);
    }
}
