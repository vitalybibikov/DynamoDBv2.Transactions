FROM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build

COPY . .

WORKDIR "/app/test-results"
WORKDIR "/test/DynamoDBv2.Transactions.IntegrationTests"
RUN dotnet build "DynamoDBv2.Transactions.IntegrationTests.csproj"

ENTRYPOINT ["dotnet", "test", "--no-build", "--logger", "trx;LogFileName=/app/test-results/testresults.trx", "/p:CollectCoverage=true", "/p:CoverletOutputFormat=cobertura", "/p:CoverletOutput=/app/test-results/coverage_integrationtests.cobertura.xml"]
