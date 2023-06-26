#ifndef __PGQMINI_H__
#define __PGQMINI_H__

#define MAX_IDENTIFIER_LENGTH 64

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <libpq-fe.h>

typedef struct
{
    PGconn *conn;
    char sub_query[512];
    char pub_query[128];
    char complete_query[128];
} PGQ;

PGQ *connect_queue(const char *host, const char *dbname, const char *user, const char *password, const char *qname, uint16_t port);
void disconnect_queue(PGQ *conn);
int pub(PGQ *conn, const char *payload);
int sub(PGQ *pgq, void (*callback)(const char *));
#endif