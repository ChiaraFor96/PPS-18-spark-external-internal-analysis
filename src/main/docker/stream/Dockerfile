FROM alpine
RUN apk add --no-cache netcat-openbsd
RUN apk add --no-cache --upgrade bash
COPY writestream.sh /writestream.sh
RUN chmod +x /writestream.sh
EXPOSE 9999
CMD ["/writestream.sh"]