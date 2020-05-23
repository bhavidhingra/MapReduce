#ifndef _MASTER_SERVER_H_
#define _MASTER_SERVER_H_

#include <string>
#include <map>
#include <set>

#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>

#define MAX_CONNECTIONS 100

enum class Opcode
{
    CLIENT_REQUEST,
    MAPPER_CONNECTION,
    REDUCER_CONNECTION,
    MAPPER_SUCCESS,
    MAPPER_FAILURE
};

class Master
{
    std::map<int, std::set<std::string>> m_job_files_map;
    std::string m_log_path;
    int mapper_sockets[MAX_CONNECTIONS];
    int reducer_sockets[MAX_CONNECTIONS];

public:

    int m_port;
    int m_sock;
    std::string m_ip_addr;
    struct sockaddr_in m_sock_address;

    Master(std::string ip = "", int p = -1): m_log_path(""), m_port(p), m_sock(-1), m_ip_addr(ip) 
    {
        for(int i = 0; i < MAX_CONNECTIONS; ++i)
        {
            mapper_sockets[i] = reducer_sockets[i] = 0;
        }
    }

    void                    run();
    void                    log_print(std::string msg);

    void                    mapper_request_handler(int, std::string);

    // Setter functions
    void                    log_path_set(std::string path) { m_log_path = path; }

    // str should be of the form "<IP Address>:<Port No.>"
    void                    ip_addr_set(std::string str) { util_ip_port_split (str, m_ip_addr, m_port); }

    void                    job_file_add   (int job_id, std::string file_path);
    int                     num_job_files  (int job_id);
    std::set<std::string>&  job_files_get  (int job_id);
    void                    job_remove     (int job_id);

    // Word Count related functions
    void                    count_all_words(int job_id);

    // Inverted Index related functions
    void                    index_all_words(int job_id);
};


#endif
