FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine3.18 AS build

COPY . .
WORKDIR "/app/test-results"
WORKDIR "/test/DynamoDBv2.Transactions.UnitTests"
RUN dotnet build "DynamoDBv2.Transactions.UnitTests.csproj"

ENTRYPOINT ["dotnet", "test", "--logger", "trx;LogFileName=/app/test-results/unittests-results.trx", "/p:CollectCoverage=true", "/p:CoverletOutputFormat=cobertura", "/p:CoverletOutput=/app/test-results/coverage_unittests.cobertura.xml"]
