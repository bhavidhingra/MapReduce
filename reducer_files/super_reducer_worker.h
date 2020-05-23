#ifndef _SUPER_REDUCER_WORKER_H_
#define _SUPER_REDUCER_WORKER_H_

#include <string>
#include <map>
#include <set>

#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>

class SuperReducer
{
    std::map<int, std::set<std::string>> m_job_files_map;
    std::string m_log_path;

public:

    int m_port;
    int m_sock;
    std::string m_ip_addr;
    struct sockaddr_in m_sock_address;

    SuperReducer(std::string ip = "", int p = -1): m_log_path(""), m_port(p), m_sock(-1), m_ip_addr(ip) {}

    void                    run();
    void                    log_print(std::string msg);

    void                    reducer_request_handler(int, std::string);

    // Setter functions
    void                    log_path_set(std::string path) { m_log_path = path; }

    // str should be of the form "<IP Address>:<Port No.>"
    void                    ip_addr_set(std::string str) { util_ip_port_split (str, m_ip_addr, m_port); }

    void                    job_file_add   (int job_id, std::string file_path);
    int                     num_job_files  (int job_id);
    std::set<std::string>&  job_files_get  (int job_id);
    void                    job_remove     (int job_id);

    // combines multiple output files to a single file
    void                    super_reduction(Problem problem, int job_id);
};



#endif
