// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

#define FREE 0
#define LOCKED 1

class lock_server {

    protected:
        int nacquire;

    private:
        pthread_mutex_t mutex;
        pthread_cond_t cond;
        std::map<lock_protocol::lockid_t, int> locks;

    public:
        lock_server();
        ~lock_server() {};
        lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
        lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
        lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif
