#!/bin/bash

dotnet tool restore
dotnet paket restore
dotnet build src/Uncodium.Storage.sln -c Debug
dotnet build src/Uncodium.Storage.sln -c Release
dotnet au run src/Uncodium.Storage.sln