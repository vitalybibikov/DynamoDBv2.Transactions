FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine3.18 AS build

COPY . .
WORKDIR "/test/DynamoDBv2.Transactions.IntegrationTests"
RUN dotnet restore "DynamoDBv2.Transactions.IntegrationTests.csproj"
RUN dotnet build "DynamoDBv2.Transactions.IntegrationTests.csproj" -c Release -o /app/build --no-restore

RUN dotnet publish "DynamoDBv2.Transactions.IntegrationTests.csproj" -c Release -o /app/publish --no-restore

WORKDIR /app/publish

ENTRYPOINT ["dotnet", "test", "DynamoDBv2.Transactions.IntegrationTests.dll", "--logger:trx"]