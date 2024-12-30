[![Dependabot Updates](https://github.com/fgrzl/woolf/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/woolf/actions/workflows/dependabot/dependabot-updates)
[![ci](https://github.com/fgrzl/woolf/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/woolf/actions/workflows/ci.yml)
[![containers](https://github.com/fgrzl/woolf/actions/workflows/containers.yml/badge.svg)](https://github.com/fgrzl/woolf/actions/workflows/containers.yml)
[![containers](https://github.com/fgrzl/woolf/actions/workflows/containers.yml/badge.svg?branch=main)](https://github.com/fgrzl/woolf/actions/workflows/containers.yml)

# Woolf
A basic streaming platform


### Develop
- first, clone the repo -> `git clone https://github.com/fgrzl/woolf`[](url)
- you need some stuff installed
  - Go
  - install protoc see this -> https://grpc.io/docs/protoc-installation/
- tidy it! -> `go mod tidy`
- go generate -> `go generate .\...` 

### Build 

```
docker buildx build . --file ./.docker/server/Dockerfile --platform linux/amd64,linux/arm64 --tag woolf.server:local
```
