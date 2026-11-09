FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 5282

ENV ASPNETCORE_URLS=http://+:5282

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG configuration=Release
WORKDIR /src
COPY ["src\KafkaPractice.Api\KafkaPractice.Api.csproj", "src/KafkaPractice.Api/"]
RUN dotnet restore "src/KafkaPractice.Api/KafkaPractice.Api.csproj"
COPY . .
WORKDIR "/src/src/KafkaPractice.Api"
RUN dotnet build "KafkaPractice.Api.csproj" -c $configuration -o /app/build

FROM build AS publish
ARG configuration=Release
RUN dotnet publish "KafkaPractice.Api.csproj" -c $configuration -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "KafkaPractice.Api.dll"]
