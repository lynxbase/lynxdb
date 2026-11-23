# Built by tools/dist/image.sh with dist/stage as its context.

# The certificate bundle is the same file on every platform, so it is taken
# from the image of the machine that builds.
FROM --platform=$BUILDPLATFORM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS certs

FROM scratch
ARG TARGETARCH
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# The mode is set here: an artifact store between the build and this file does
# not keep it.
COPY --chmod=0755 linux-${TARGETARCH}/lynxdb /usr/local/bin/lynxdb
EXPOSE 3100
ENTRYPOINT ["/usr/local/bin/lynxdb"]
CMD ["server"]
