#ifndef _SUPER_REDUCER_H_
#define _SUPER_REDUCER_H_

#include <string>
#include <map>
#include <set>
#include "utilities.h"

class SuperReducer
{
    int sock;

public:

    int port;
    std::string ip_addr;

    SuperReducer(std::string ip = "", int p = -1): sock(-1), port(p), ip_addr(ip)
    {
        sock = util_connection_make(ip_addr, port);
    }

    int sock_get() { return sock; }

    int request_super_reduction(Problem problem, int job_id, std::string file_path);
};


#endif  /* _SUPER_REDUCER_H_ */
