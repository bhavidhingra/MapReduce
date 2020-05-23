#include "includes.h"
#include "utilities.h"

#include <cstdio>
#include <cassert>
#include <openssl/sha.h>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>
#include <sstream>

#include <string.h> 
#include <signal.h>

#include "utilities.h"
#include "master_server.h"

using namespace std;

#define PIECE_SIZE     (512 * 1024)
#define pb             push_back

#define MAX_DOWNLOADS       100

static int            num_mappers_alive = MAX_MAPPERS;

string working_dir;


// Assumptions
int num_reducers = 5;
int num_mappers = 4;


struct Job
{
    vector<string> category[num_reducers];
};

Job job1;

string current_timestamp_get()
{
    time_t tt;
    struct tm *ti;

    time (&tt);
    ti = localtime(&tt);
    return asctime(ti);
}


void Master::log_print(string msg)
{
    ofstream out(m_log_path, ios_base::app);
    if(!out)
    {
        stringstream ss;
        ss << "[Master] Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        //status_print(FAILURE, ss.str());
        return;
    }
    string curr_timestamp = current_timestamp_get();
    curr_timestamp.pop_back();
    out << "[Master] " << curr_timestamp << " : " << "\"" << msg << "\"" << "\n";
}


void Master::job_remove(int job_id)
{
    m_job_files_map[job_id].clear();
    m_job_files_map.erase(job_id);
}


int command_size_check(vector<string> &v, unsigned int min_size, unsigned int max_size, string error_msg)
{
    if(v.size() < min_size || v.size() > max_size)
    {
        //status_print(FAILURE, error_msg);
        return FAILURE;
    }
    return SUCCESS;
}

void Master::job_file_add(int job_id, string file_path)
{
    set<string>& files_set = m_job_files_map[job_id];
    int num_files = files_set.size();
    // check number of files received for this job
    assert(num_files < num_mappers_alive);

    files_set.insert(file_path);
}

int Master::num_job_files(int job_id)
{
    set<string>& files_set = m_job_files_map[job_id];
    return files_set.size();
}


set<string>& Master::job_files_get(int job_id)
{
    return m_job_files_map[job_id];
}


void Master::count_all_words(int job_id)
{
    map<string, int> word_count_map;
    set<string>&     input_files_set = job_files_get(job_id);
    string           word;
    int              count = 0;

    for(auto itr = input_files_set.begin(); itr != input_files_set.end(); ++itr)
    {
        ifstream in(*itr);
        if(!in)
        {
            stringstream ss;
            ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << (*itr) << " : " << strerror(errno);
            log_print(ss.str());
            continue;
        }
        while(in >> word >> count)
        {
            word_count_map[word] += count;
        }
    }

    string temp_file = "word_cout_result_" + to_string(job_id) + ".txt";
    ofstream out(temp_file);
    if(!out)
    {
        stringstream ss;
        ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        log_print(ss.str());
        return;
    }
    for(auto itr = word_count_map.begin(); itr != word_count_map.end(); ++itr)
    {
        out << itr->first << " " << itr->second << "\n";
    }

    job_remove(job_id);
}


void Master::index_all_words(int job_id)
{
    map<string, set<string>> word_files_map;
    set<string>&             input_files_set = job_files_get(job_id);
    string                   line, word, file_path;

    for(auto itr = input_files_set.begin(); itr != input_files_set.end(); ++itr)
    {
        ifstream in(*itr);
        if(!in)
        {
            stringstream ss;
            ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << (*itr) << " : " << strerror(errno);
            log_print(ss.str());
            continue;
        }
        while(getline(in, line))
        {
            istringstream iss(line);
            iss >> word;
            while(iss >> file_path)
            {
                word_files_map[word].insert(file_path);
            }
        }
    }

    string temp_file = "invertex_index_result_" + to_string(job_id) + ".txt";
    ofstream out(temp_file);
    if(!out)
    {
        stringstream ss;
        ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        log_print(ss.str());
        return;
    }
    for(auto mitr = word_files_map.begin(); mitr != word_files_map.end(); ++mitr)
    {
        set<string>& word_files_set = mitr->second;
        out << mitr->first;
        for(auto sitr = word_files_set.begin(); sitr != word_files_set.end(); ++sitr)
        {
            out << " " << *sitr;
        }
        out << "\n";
    }

    job_remove(job_id);
}


void Master::request_handler(int sock, string req_str)
{
    stringstream ss;
    ss << "Handling request: " << req_str;
    log_print(ss.str());

    vector<string> tokens_vec;
    string token;
    stringstream ss(req_str);

    // Tokenizing w.r.t. space '$'
    while(getline(ss, token, '$')) 
    { 
        tokens_vec.push_back(token); 
    } 


    // currently only taking "opcode$..." in the request buffer
    int opcode = stoi(tokens_vec[0]);

    switch(opcode)
    {
        case Opcode::CLIENT_REQUEST:
        {
            dollar_pos = req_str.find('$');
            string problem_str = req_str.substr(0, dollar_pos);
            Problem problem = (Problem)(stoi(problem_str));
            req_str.erase(0, dollar_pos+1);

            case Problem::WORD_COUNT:
            {
                string input_file_path = req_str;
                break;
            }

            case Problem::INVERTED_INDEX:
            {
                break;
            }
            break;
        }

        case MAPPER_CONENECTION:
        {
            for (int i = 0; i < MAX_CONNECTIONS; ++i)
            {
                if(incoming_connections[i] == sock)
                {
                    incoming_connections[i] = 0;
                    break;
                }
            }
            for (int i = 0; i < MAX_CONNECTIONS; ++i)
            {
                if(mapper_sockets[i] == 0)
                {
                    mapper_sockets[i] = sock;
                    break;
                }
            }
            break;
        }

        case REDUCER_CONNECTION:
        {
            for (int i = 0; i < MAX_CONNECTIONS; ++i)
            {
                if(incoming_connections[i] == sock)
                {
                    incoming_connections[i] = 0;
                    break;
                }
            }
            for (int i = 0; i < MAX_CONNECTIONS; ++i)
            {
                if(reducer_sockets[i] == 0)
                {
                    mapper_sockets[i] = sock;
                    break;
                }
            }
            break;
        }

        default:
            break;
    }
}


void Master::response_handler(int sock, string response_str)
{
    stringstream ss;
    ss << "Handling response: " << response_str << endl;
    log_print(ss.str());

    // currently only taking "opcode$..." in the request buffer
    int dollar_pos = response_str.find('$');
    string opcode_str = response_str.substr(0, dollar_pos);
    int opcode = stoi(opcode_str);
    response_str.erase(0, dollar_pos+1);

    switch(opcode)
    {
        case MAPPER_SUCCESS:
        {
            dollar_pos = response_str.find('$');
            string job_id_str = response_str.substr(0, dollar_pos);
            int job_id = stoi(job_id_str);
            response_str.erase(0, dollar_pos+1);

            string file_path;
            int i = 0;
            while(1)
            {
                dollar_pos = response_str.find('$');
                file_path = response_str.substr(0, dollar_pos);
                if (dollor_pos == response_str.length())
                {
                    break;
                }
                response_str.erase(0, dollar_pos+1);

                category[i].push_back(file_path);
                send_to_reducer(reducer_sockets[reducer_of_category[i]], job_id
            }

            Problem problem = (Problem)(stoi(problem_str));
            response_str.erase(0, dollar_pos+1);

            case Problem::WORD_COUNT:
            {
                string input_file_path = response_str;
                break;
            }

            case Problem::INVERTED_INDEX:
            {
                break;
            }
            break;
        }

        case MAPPER_FAILURE:
        {
            break;
        }

        default:
            break;
    }
}


void Master::run()
{
    int opt = true;
    int addrlen , new_sock , client_sockets[MAX_CONNECTIONS],
        activity, valread , sd, mapper_sd, reducer_sd;                 // sd is used for general connections
    int max_sd;

    fd_set readfds;

    for (int i = 0; i < MAX_CONNECTIONS; i++)
    {
        client_sockets[i] = 0;
    }

    if((m_sock = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): socket failed!!";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // set master's socket to allow multiple connections from clients, mappers and reducers
    // this is just a good habit, it will work without this
    if( setsockopt(m_sock, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): setsockopt";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // type of socket created  
    m_sock_address.sin_family = AF_INET;
    m_sock_address.sin_addr.s_addr = INADDR_ANY;
    m_sock_address.sin_port = htons(m_port);

    // bind the socket to r_tracker's port
    if (bind(m_sock, (struct sockaddr *)&m_sock_address, sizeof(m_sock_address))<0)   
    {   
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): bind failed";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }   
    stringstream ss;
    ss << "Master listening on " << m_ip_addr << ":" << m_port;
    log_print(ss.str());
         
    // try to specify maximum of pending connections for r_tracker's socket
    if (listen(m_sock, MAX_CONNECTIONS) < 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): listen failed";
        log_print(ss.str());
        exit(EXIT_FAILURE);   
    }   

    // accept the incoming connection  
    addrlen = sizeof(m_sock_address);

    while(true)
    {   
        // clear the socket set
        FD_ZERO(&readfds);   

        // add r_tracker's socket to the read fd set
        FD_SET(m_sock, &readfds);
        max_sd = m_sock;

        // add mapper sockets to set
        for (int i = 0 ; i < MAX_CONNECTIONS ; i++)
        {
            // socket descriptor
            sd = client_sockets[i];
            mapper_sd = mapper_socket[i];
            reducer_sd = reducer_socket[i];

            // if valid socket descriptor then add to read fd list
            if(sd > 0)
            {
                FD_SET(sd , &readfds);

                // highest file descriptor number, need it for the select function
                if(sd > max_sd)
                    max_sd = sd;
            }
            if(mapper_sd > 0)
            {
                FD_SET(mapper_sd , &readfds);

                // highest file descriptor number, need it for the select function
                if(mapper_sd > max_sd)
                    max_sd = mapper_sd;
            }
            if(reducer_sd > 0)
            {
                FD_SET(reducer_sd , &readfds);

                // highest file descriptor number, need it for the select function
                if(reducer_sd > max_sd)
                    max_sd = reducer_sd;
            }
        }

        // wait for an activity on one of the sockets , timeout is NULL,
        // so wait indefinitely
        activity = select(max_sd + 1 , &readfds , NULL , NULL , NULL);

        if (activity < 0)
        {
            if(errno != EINTR)
            {
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): select error: " << errno;
                log_print(ss.str());
            }
            continue;
        }

        // If something happened on the master socket ,  
        // then its an incoming connection  
        if (FD_ISSET(m_sock, &readfds))   
        {
            new_sock = accept(m_sock, (struct sockaddr *) &m_sock_address, (socklen_t*) &addrlen);
            if(FAILURE == new_sock)
            {
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): accept() failed!!";
                log_print(ss.str());
                exit(EXIT_FAILURE);
            }

            // add new socket to array of sockets
            for (int i = 0; i < MAX_CONNECTIONS; i++)
            {
                // if position is empty
                if(client_sockets[i] == 0)
                {
                    client_sockets[i] = new_sock;

                    stringstream ss;
                    ss << "New client with socket id " << new_sock << " connected\n";
                    log_print(ss.str());
                    break;
                }
            }
        }
        else
        {
            // else its some IO operation on some other socket
            for (int i = 0; i < MAX_CONNECTIONS; i++)
            {
                sd = client_sockets[i];
                if (FD_ISSET(sd , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sd, buffer_str, error_msg))
                    {
                        log_print(error_msg);
                        close(sd);
                        client_socks[i] = 0;
                        continue;
                    }

                    request_handler(sd, buffer_str);
                }
            }
            for (int i = 0; i < MAX_CONNECTIONS; i++)
            {
                mapper_sd = mapper_sockets[i];
                if (FD_ISSET(mapper_sd , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sd, buffer_str, error_msg))
                    {
                        log_print(error_msg);
                        continue;
                    }

                    response_handler(mapper_sd, buffer_str);
                }
            }

        }
    }
}

int main(int argc, char* argv[])
{
    if(argc != 3)
    {
        //status_print(FAILURE, "Reducer Launch command : \"/master <REDUCER_IP>:<PORT> <master_log_file>\"");
        //cout << endl;
        return 0;
    }

    Master master;
    master.log_path_set(util_abs_path_get(argv[2]));         // USE NFS PATH HERE
    master.ip_addr_set(argv[1]);

    master.run();

    return 0;
}
