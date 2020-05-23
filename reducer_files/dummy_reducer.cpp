#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "super_reducer_client.h"

using namespace std;

string working_dir;

int main(int argc, char* argv[])
{
    string r_ip_addr;
    int r_port;
    int job_id;

    if(argc != 5)
    {
        //status_print(FAILURE, "Reducer Usage : \"./dummy_reducer <problem_id> <job_id> <super_educer_ip:port> <file_path/name>\"");
        //cout << endl;
        return 0;
    }

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    job_id = atoi(argv[2]);

    util_ip_port_split(argv[3], r_ip_addr, r_port);

    SuperReducer super_reducer (r_ip_addr, r_port);

    if(FAILURE == super_reducer.sock_get())
        return FAILURE;

    string file_path = util_abs_path_get(argv[4]);

    Problem problem = (atoi(argv[1]) == 0) ? Problem::WORD_COUNT : Problem::INVERTED_INDEX;
    super_reducer.request_super_reduction(problem, job_id, file_path);

    return 0;
}
