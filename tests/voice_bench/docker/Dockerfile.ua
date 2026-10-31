# =============================================================================
# srw-voice-bench-ua — scriptable SIP user agent for the WP0 phone test bench
# =============================================================================
# pjsua (pjproject's reference UA) plus ua_agent.py, a stdlib-only controller
# that drives pjsua's line-based console: auto-answer, play a WAV in a loop,
# record what the far end sends, send DTMF (RFC 4733 or SIP INFO), blind
# transfer (REFER), hang up on a timeline, and write every SIP message, call
# state, received DTMF digit and transfer result as JSON lines.
#
# The pjproject tarball is the same pinned artifact Asterisk 22.11.0 bundles
# (Asterisk's third-party mirror; md5 also shipped in the signed Asterisk tree).
# Codecs: G.711 A-law/u-law, G.722 and telephone-event only. No sound device
# (null audio), no video, no TLS/SRTP. pjsua is GPL-2.0; it runs in this test
# container only and is never linked into SRW.
#
#   docker build -f tests/voice_bench/docker/Dockerfile.ua \
#       -t srw-voice-bench-ua:pjsua-2.17 tests/voice_bench
# =============================================================================
FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS build

ARG PJPROJECT_VERSION=2.17
ARG PJPROJECT_SHA256=04b2eb1f0f01aa0ad1945b167171843448a51aa6b7c3e806496d434f13a112b7

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl bzip2; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
RUN set -eux; \
    pj="pjproject-${PJPROJECT_VERSION}.tar.bz2"; \
    curl -fsSLo "$pj" \
        "https://raw.githubusercontent.com/asterisk/third-party/master/pjproject/${PJPROJECT_VERSION}/$pj"; \
    echo "${PJPROJECT_SHA256}  $pj" | sha256sum -c -; \
    tar xjf "$pj"; \
    cd "pjproject-${PJPROJECT_VERSION}"; \
    ./configure --disable-sound --disable-video --disable-ssl \
        --disable-libsrtp --disable-speex-codec --disable-speex-aec \
        --disable-ilbc-codec --disable-gsm-codec --disable-l16-codec \
        --disable-g7221-codec --disable-opus --disable-silk --disable-bcg729 \
        --disable-opencore-amr --disable-libyuv --disable-libwebrtc \
        --disable-openh264 --disable-ffmpeg --disable-v4l2 --disable-sdl \
        --disable-vpx --disable-upnp --disable-pjsua2 > /tmp/configure.log; \
    make dep > /tmp/make-dep.log 2>&1; \
    make -j"$(nproc)" > /tmp/make.log 2>&1 || { tail -60 /tmp/make.log; exit 1; }; \
    install -m 0755 pjsip-apps/bin/pjsua-* /usr/local/bin/pjsua; \
    /usr/local/bin/pjsua --version

FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad

LABEL org.opencontainers.image.title="srw-voice-bench-ua" \
      org.opencontainers.image.description="pjsua 2.17 + controller for SRW's WP0 phone test bench"

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3; \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/pjsua /usr/local/bin/pjsua
COPY ua/ua_agent.py /usr/local/bin/ua_agent.py

# The bench runs this container as the invoking user's uid so the recordings
# and logs it writes into the bind-mounted run directory stay owned by them.
USER 65532:65532
ENTRYPOINT ["python3", "-u", "/usr/local/bin/ua_agent.py"]
