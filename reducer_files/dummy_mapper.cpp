#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "reducer_server.h"

using namespace std;

string working_dir;

int main(int argc, char* argv[])
{
    string r_ip_addr;
    int r_port;
    int job_id;

    if(argc != 5)
    {
        //status_print(FAILURE, "Mapper Usage : \"./dummy_mapper <problem_id> <job_id> <reducer_ip:port> <file_path/name>\"");
        //cout << endl;
        return 0;
    }

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    job_id = atoi(argv[2]);

    util_ip_port_split(argv[3], r_ip_addr, r_port);

    ReducerTracker reducer (r_ip_addr, r_port);

    if(FAILURE == reducer.sock_get())
        return FAILURE;

    string file_path = util_abs_path_get(argv[4]);

    if (atoi(argv[1]) == 0)
        reducer.request_send(Problem::WORD_COUNT, job_id, file_path);
    else
        reducer.request_send(Problem::INVERTED_INDEX, job_id, file_path);

    return 0;
}
