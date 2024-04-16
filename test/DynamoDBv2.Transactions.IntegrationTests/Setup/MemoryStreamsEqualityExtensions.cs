using System;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Setup
{
    public static class MemoryStreamsEquality
    {
        public static void StreamEqual(MemoryStream stream1, MemoryStream stream2)
        {
            stream1.Position = 0;
            stream2.Position = 0;

            var equal = false;
            byte[] buffer1 = stream1.ToArray();
            byte[] buffer2 = stream2.ToArray();

            for (int i = 0; i < buffer1.Length; i++)
            {
                if (buffer1[i] != buffer2[i])
                {
                    Assert.Fail("Streams are not equal");
                }
            }

            equal = true;

            Assert.True(equal);
        }
    }
}
