using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DynamoDBv2.Transactions.IntegrationTests.Helpers
{
    using System;
    using System.Collections.Generic;
    using Xunit;

    public class DateTimeComparer : IEqualityComparer<DateTime>
    {
        private readonly TimeSpan _tolerance;

        public DateTimeComparer(TimeSpan tolerance)
        {
            _tolerance = tolerance;
        }

        public bool Equals(DateTime x, DateTime y)
        {
            // Convert both dates to UTC to compare
            DateTime utcX = x.ToUniversalTime();
            DateTime utcY = y.ToUniversalTime();

            // Check if the difference between dates is within the tolerance
            return Math.Abs((utcX - utcY).Ticks) <= _tolerance.Ticks;
        }

        public int GetHashCode(DateTime obj)
        {
            return obj.ToUniversalTime().GetHashCode();
        }
    }
}
