// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <vector>
#include <fstream>

extent_client::extent_client(std::string dst)
{
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
    t_num = 0;
}

// a demo to show how to use RPC
extent_protocol::status extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr)
{
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::getattr, eid, attr);
    return ret;
}

extent_protocol::status extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::create, type, id);

    std::stringstream ss;
    ss << t_num << " create " << id;
    std::string log_str;
    ss >> log_str;
    log_str = ss.str();
    log(log_str);
    return ret;
}

extent_protocol::status extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::get, eid, buf);
    return ret;
}

extent_protocol::status extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
    extent_protocol::status ret = extent_protocol::OK;
    std::stringstream ss;
    ss << t_num << " put " << eid << " " << buf.size() << ' ' << buf;

    std::string log_str = ss.str();
    ret = log(log_str);
    return ret;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t eid)
{
    extent_protocol::status ret = extent_protocol::OK;
    std::stringstream ss;
    ss << t_num << " remove " << eid;

    std::string log_str = ss.str();
    ret = log(log_str);
    return ret;
}

extent_protocol::status extent_client::log(std::string log)
{
    extent_protocol::status ret = extent_protocol::OK;

    if (log == "begin") {
        ret = cl->call(extent_protocol::log, log, t_num);
        return ret;
    } else if (log == "end"){
        std::stringstream ss;
        ss << t_num << ' ' << log;
        log = ss.str();
    }

    int r;
    ret = cl->call(extent_protocol::log, log, r);
    return ret;
}

extent_protocol::status extent_client::commit()
{
    int m, n;
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::commit, m, n);
    return ret;
}

extent_protocol::status extent_client::undo()
{
    int m, n;
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::undo, m, n);
    return ret;
}

extent_protocol::status extent_client::redo()
{
    int m, n;
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::redo, m, n);
    return ret;
}
