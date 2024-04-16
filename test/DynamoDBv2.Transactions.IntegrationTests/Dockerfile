FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine3.18 AS build

COPY . .
WORKDIR "/app/test-results"
WORKDIR "/test/DynamoDBv2.Transactions.IntegrationTests"
RUN dotnet build "DynamoDBv2.Transactions.IntegrationTests.csproj"

ENTRYPOINT ["dotnet", "test", "--logger", "trx;LogFileName=/app/test-results/testresults.trx"]