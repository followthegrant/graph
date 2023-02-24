FROM ghcr.io/investigativedata/ftm-docker:main

LABEL org.opencontainers.image.title "FollowTheGrant Graph ETL"
LABEL org.opencontainers.image.source https://github.com/followthegrant/graph

RUN mkdir -p /graph
COPY . /graph
RUN pip install -e /graph
WORKDIR /graph
CMD ["bash"]
