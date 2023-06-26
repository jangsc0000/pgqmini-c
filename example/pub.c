#include "pgqmini.h"

int main()
{
    PGQ *pgq = connect_queue("localhost", "dbname", "username", "password", "qname", 5432);
    if (!pgq)
    {
        printf("connect fail");
        exit(1);
    }
    pub(pgq, "{\"test\":\"testvalue\"}");

    disconnect_queue(pgq);
}
