FROM ghcr.io/julea-io/julea-ubuntu-config:latest

COPY docker-entrypoint.sh /entrypoint.sh
EXPOSE 9876/tcp

CMD ["juleaServer", "9876"]
ENTRYPOINT ["/entrypoint.sh"]
