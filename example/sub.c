
#include "pgqmini.h"

void process_message(const char *payload)
{
    printf("%s\n", payload);
    // process
    sleep(2);
}

int main()
{
    PGQ *pgq = connect_queue("localhost", "dbname", "username", "password", "qname", 5432);
    if (!pgq)
    {
        printf("connect fail");
        exit(1);
    }
    while (sub(pgq, process_message))
        ;

    disconnect_queue(pgq);
}
