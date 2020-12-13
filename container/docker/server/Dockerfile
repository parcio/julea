FROM ghcr.io/vittel1/julea-config:latest

COPY docker-entrypoint.sh /entrypoint.sh
EXPOSE 9876

CMD ["juleaServer", "9876"]
ENTRYPOINT ["/entrypoint.sh"]
