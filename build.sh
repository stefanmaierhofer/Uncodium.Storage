#!/bin/bash

dotnet tool restore
dotnet paket restore
dotnet build src/Uncodium.Storage.sln -c Release