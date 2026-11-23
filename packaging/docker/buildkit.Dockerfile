# Not built: tools/dist/image.sh reads from here which BuildKit builds and
# pushes the image of a release, and Dependabot keeps it current.
FROM moby/buildkit:v0.35.1@sha256:baf6138eb8c37644800c7c913fba051e840ebb9ba13eaf92d92ef9a1712d0b4c
