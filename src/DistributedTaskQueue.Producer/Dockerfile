# =========================
# Build stage
# =========================
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Copy solution files
COPY ["DistributedTaskQueue.sln", "./"]
COPY ["src/DistributedTaskQueue.Core/DistributedTaskQueue.Core.csproj", "src/DistributedTaskQueue.Core/"]
COPY ["src/DistributedTaskQueue.Infrastructure/DistributedTaskQueue.Infrastructure.csproj", "src/DistributedTaskQueue.Infrastructure/"]
COPY ["src/DistributedTaskQueue.Producer/DistributedTaskQueue.Producer.csproj", "src/DistributedTaskQueue.Producer/"]

# Restore dependencies
RUN dotnet restore "src/DistributedTaskQueue.Producer/DistributedTaskQueue.Producer.csproj"

# Copy everything else
COPY . .

# Publish
RUN dotnet publish "src/DistributedTaskQueue.Producer/DistributedTaskQueue.Producer.csproj" \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

# =========================
# Runtime stage
# =========================
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 8080

ENTRYPOINT ["dotnet", "DistributedTaskQueue.Producer.dll"]
