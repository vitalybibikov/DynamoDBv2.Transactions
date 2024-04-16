using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Setup
{
    [CollectionDefinition("DynamoDb")]
    public class DatabaseCollection : ICollectionFixture<DatabaseFixture>
    {
    }
}
