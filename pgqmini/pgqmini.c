
#include "pgqmini.h"

#define RESULT_OK (1)
#define RESULT_FAIL (0)

const char *CREATE_DATABASE_QUERY = "CREATE DATABASE %s;";

const char *CREATE_STATUS_TYPE_QUERY =
    "CREATE TYPE message_status AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED');";

const char *CREATE_QUEUE_TABLE_QUERY =
    "CREATE TABLE %s ( \n \
        id SERIAL PRIMARY KEY, \n \
        payload JSON NOT NULL, \n \
        status message_status NOT NULL DEFAULT 'PENDING', \n \
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, \n \
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP \n \
    );";

const char *CREATE_STATUS_INDEX_QUERY =
    "CREATE INDEX idx_%s_status ON %s(status);";

const char *CREATE_UPDATED_AT_INDEX_QUERY =
    "CREATE INDEX idx_%s_updated_at ON %s(updated_at);";

const char *CREATE_UPDATE_FUNCTION_QUERY =
    "CREATE OR REPLACE FUNCTION update_updated_at_column() \n \
    RETURNS TRIGGER AS $$ \n \
    BEGIN \n \
        NEW.updated_at = NOW(); \n \
        IF (NEW.status = 'PENDING' AND NEW.status <> OLD.status) THEN \n \
            PERFORM pg_notify('new_message', NEW.id::text); \n \
        END IF; \n \
        RETURN NEW; \n \
    END; \n \
    $$ language 'plpgsql';";

const char *CREATE_UPDATE_TRIGGER_QUERY =
    "CREATE OR REPLACE TRIGGER update_%s_updated_at \n \
    BEFORE UPDATE ON %s \n \
    FOR EACH ROW \n \
    EXECUTE PROCEDURE update_updated_at_column();";

const char *CREATE_NOTIFY_FUNCTION_QUERY =
    "CREATE OR REPLACE FUNCTION notify_insert_message() RETURNS TRIGGER AS $$ \n \
    DECLARE \n \
    BEGIN \n \
    PERFORM pg_notify('new_message', NEW.id::text); \n \
    RETURN NEW; \n \
    END; \n \
    $$ LANGUAGE plpgsql;";

const char *CREATE_INSERT_TRIGGER_QUERY =
    "CREATE OR REPLACE TRIGGER %s_trigger \n \
    AFTER INSERT ON %s \n \
    FOR EACH ROW EXECUTE PROCEDURE notify_insert_message();";

const char *INSERT_MESSAGE_QUERY =
    "INSERT INTO %s (payload) VALUES ($1)";

const char *SELECT_MESSAGE_QUERY =
    "UPDATE %s \
    SET status = 'PROCESSING' \
    FROM ( \
    SELECT id FROM %s \
    WHERE status = 'PENDING' \
    ORDER BY id \
    FOR UPDATE SKIP LOCKED \
    LIMIT 1 \
    ) sub \
    WHERE %s.id = sub.id \
    RETURNING %s.id, payload";

const char *COMPLETE_QUERY =
    "UPDATE %s \
    SET status = 'COMPLETED' \
    WHERE id = $1";

const char *LISTEN_QUERY = "LISTEN new_message";

static void create_queue(PGconn *conn, const char *qname)
{
    char query[350];

    PGresult *res = PQexec(conn, CREATE_STATUS_TYPE_QUERY);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_STATUS_TYPE_QUERY failed: %s", PQerrorMessage(conn));

    sprintf(query, CREATE_QUEUE_TABLE_QUERY, qname);
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_QUEUE_TABLE_QUERY failed: %s", PQerrorMessage(conn));

    sprintf(query, CREATE_STATUS_INDEX_QUERY, qname, qname);
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_STATUS_INDEX_QUERY failed: %s", PQerrorMessage(conn));

    sprintf(query, CREATE_UPDATED_AT_INDEX_QUERY, qname, qname);
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_UPDATED_AT_INDEX_QUERY failed: %s", PQerrorMessage(conn));

    res = PQexec(conn, CREATE_UPDATE_FUNCTION_QUERY);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_UPDATE_FUNCTION_QUERY failed: %s", PQerrorMessage(conn));

    sprintf(query, CREATE_UPDATE_TRIGGER_QUERY, qname, qname);
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_UPDATE_TRIGGER_QUERY failed: %s", PQerrorMessage(conn));

    res = PQexec(conn, CREATE_NOTIFY_FUNCTION_QUERY);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_NOTIFY_FUNCTION_QUERY failed: %s", PQerrorMessage(conn));

    sprintf(query, CREATE_INSERT_TRIGGER_QUERY, qname, qname);
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "CREATE_INSERT_TRIGGER_QUERY failed: %s", PQerrorMessage(conn));
}

static PGconn *connect_db(const char *host, const char *dbname, const char *user, const char *password, uint16_t port)
{
    char conninfo[320];
    sprintf(conninfo, "host=%s port=%d dbname=%s user=%s password=%s", host, port, dbname, user, password);

    PGconn *conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return RESULT_FAIL;
    }

    return conn;
}

static int create_db(const char *host, const char *dbname, const char *user, const char *password, uint16_t port)
{
    PGconn *conn = connect_db(host, "postgres", user, password, port);
    if (!conn)
        return RESULT_FAIL;

    char query[128];
    sprintf(query, CREATE_DATABASE_QUERY, dbname);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "Database creation failed: %s", PQerrorMessage(conn));
    PQclear(res);

    PQfinish(conn);
    return RESULT_OK;
}

static void complete(PGQ *pgq, int id)
{
    const char *paramValues[1];
    char id_str[10];
    sprintf(id_str, "%d", id);
    paramValues[0] = id_str;

    PGresult *res = PQexecParams(pgq->conn, pgq->complete_query, 1, NULL, paramValues, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        fprintf(stderr, "COMPLETE QUERY failed: %s", PQerrorMessage(pgq->conn));
    PQclear(res);
}

PGQ *connect_queue(const char *host, const char *dbname, const char *user, const char *password, const char *qname, uint16_t port)
{
    if (!create_db(host, dbname, user, password, port))
        return RESULT_FAIL;

    PGconn *conn = connect_db(host, dbname, user, password, port);
    if (!conn)
        return RESULT_FAIL;

    create_queue(conn, qname);

    PGQ *pgq = malloc(sizeof(PGQ));
    if (!pgq)
        return RESULT_FAIL;

    pgq->conn = conn;

    sprintf(pgq->sub_query, SELECT_MESSAGE_QUERY, qname, qname, qname, qname);
    sprintf(pgq->pub_query, INSERT_MESSAGE_QUERY, qname);
    sprintf(pgq->complete_query, COMPLETE_QUERY, qname);

    return pgq;
}

void disconnect_queue(PGQ *pgq)
{
    PQfinish(pgq->conn);
    free(pgq);
}

int pub(PGQ *pgq, const char *payload)
{
    const char *paramValues[1];
    paramValues[0] = payload;

    PGresult *res = PQexecParams(pgq->conn, pgq->pub_query, 1, NULL, paramValues, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "INSERT_MESSAGE_QUERY failed: %s", PQerrorMessage(pgq->conn));
        PQclear(res);
        return RESULT_FAIL;
    }

    PQclear(res);
    return RESULT_OK;
}

int sub(PGQ *pgq, void (*callback)(const char *))
{
    PQexec(pgq->conn, LISTEN_QUERY);

    while (1)
    {
        PGresult *res = PQexec(pgq->conn, pgq->sub_query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            fprintf(stderr, "SELECT_MESSAGE_QUERY failed: %s", PQerrorMessage(pgq->conn));
            PQclear(res);
            return RESULT_FAIL;
        }

        int rows = PQntuples(res);
        if (rows < 1)
        {
            fd_set input_mask;
            int sock = PQsocket(pgq->conn);
            FD_ZERO(&input_mask);
            FD_SET(sock, &input_mask);

            // Wait for NOTIFY event
            if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
            {
                fprintf(stderr, "select() failed\n");
                return RESULT_FAIL;
            }
            continue;
        }

        int id = atoi(PQgetvalue(res, 0, 0));
        char *payload = PQgetvalue(res, 0, 1);

        callback(payload);

        PQclear(res);

        complete(pgq, id);
        return RESULT_OK;
    }
}
