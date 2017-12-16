// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
  ec = NULL;
  lc = NULL;
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst, const char* cert_file)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}

int
yfs_client::verify(const char* name, unsigned short *uid)
{
  	int ret = OK;

	return ret;
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

/* Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 */

bool yfs_client::issymlink(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }
    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
}

int yfs_client::getsymlink(inum inum, symlinkinfo &sin) {
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size = a.size;
    printf("getsymlink %016llx -> sz %llu\n", inum, sin.size);

release:
    return r;
}

int yfs_client::symlink(const char *link, inum parent, const char *name, inum &ino_out) {
    lc->acquire(parent);
    int r = OK;
    bool found = false;
    std::string buf;

    if (!isdir(parent)) return IOERR;
    lookup(parent, name, found, ino_out);
    if (found) return EXIST;

    ec->log("begin");
    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK) {
        printf("symlink - create error\n");
        return IOERR;
    }
    if (ec->put(ino_out, std::string(link)) != extent_protocol::OK) {
        printf("symlink - put error\n");
        return IOERR;
    }
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("symlink - get error\n");
        return IOERR;
    }
    buf += std::string(name) + "//" + filename(ino_out) + "//";
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("symlink - put error\n");
        return IOERR;
    }
    ec->log("end");
    lc->release(parent);
    return r;
}

int yfs_client::readlink(inum ino, std::string &link) {
    int r = OK;
    if (!issymlink(ino)) return IOERR;
    if (ec->get(ino, link) != extent_protocol::OK) {
        printf("readlink - get error\n");
        return IOERR;
    }
    return r;
}


bool yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a directory\n", inum);
        return true;
    }
    printf("isdir: %lld is not a directory\n", inum);
    return false;
}

int yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    lc->acquire(ino);
    extent_protocol::attr a;
    std::string buf;

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        printf("setattr - getattr error\n");
        lc->release(ino);
        return IOERR;
    }
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("setattr - get error\n");
        lc->release(ino);
        return IOERR;
    }
    if (buf.length() > size) {
        buf.erase(size);
    } else {
        buf.resize(size);
    }
    ec->log("begin");
    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("setattr - put error\n");
        lc->release(ino);
        return IOERR;
    }
    ec->log("end");
    lc->release(ino);
    return r;
}

int yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    lc->acquire(parent);
    bool found = false;
    std::string buf;

    if (!isdir(parent)) {
        lc->release(parent);
        return IOERR;
    }
    lookup(parent, name, found, ino_out);
    if (found) {
        lc->release(parent);
        return EXIST;
    }

    ec->log("begin");
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        printf("create - create error\n");
        lc->release(parent);
        return IOERR;
    }
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("create - get error\n");
        lc->release(parent);
        return IOERR;
    }
    buf += std::string(name) + "//" + filename(ino_out) + "//";
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("create - put error\n");
        lc->release(parent);
        return IOERR;
    }
    ec->log("end");
    lc->release(parent);
    return r;
}

int yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    lc->acquire(parent);
    bool found = false;
    std::string buf;

    if (!isdir(parent)) {
        lc->release(parent);
        return IOERR;
    }
    lookup(parent, name, found, ino_out);
    if (found) {
        lc->release(parent);
        return EXIST;
    }

    ec->log("begin");
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        printf("mkdir - create error\n");
        lc->release(parent);
        return IOERR;
    }
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("mkdir - get error\n");
        lc->release(parent);
        return IOERR;
    }
    buf += std::string(name) + "//" + filename(ino_out) + "//";
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("mkdir - put error\n");
        lc->release(parent);
        return IOERR;
    }
    ec->log("end");
    lc->release(parent);
    return r;
}

int yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    found = false;
    std::string buf;
    std::list<dirent> dirent_list;

    if (!isdir(parent)) return IOERR;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("lookup - get error\n");
        return IOERR;
    }
    readdir(parent, dirent_list);
    for (std::list<dirent>::iterator it = dirent_list.begin(); it != dirent_list.end(); it++) {
        if (std::string(name) == it->name){
            found = true;
            ino_out = it->inum;
            break;
        }
    }

    return r;
}

int yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    std::string buf;

    if (!isdir(dir)) return IOERR;
    if (ec->get(dir, buf) != extent_protocol::OK) {
        printf("readdir - get error\n");
        return IOERR;
    }
    uint32_t ptr = 0;
    uint32_t size = buf.length();
    while (ptr < size) {
        dirent dirents;
        uint32_t tmp = buf.find("//", ptr);
        dirents.name = buf.substr(ptr, tmp - ptr);
        ptr = tmp + 2;
        tmp = buf.find("//", ptr);
        dirents.inum = n2i(buf.substr(ptr, tmp - ptr));
        ptr = tmp + 2;
        list.push_back(dirents);
    }
    return r;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("read - get error\n");
        return IOERR;
    }
    data = off > (off_t)buf.length() ? "" : buf.substr(off, size);
    return r;
}

int yfs_client::write(inum ino, size_t size, off_t off, const char *data, size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    lc->acquire(ino);
    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("write - get error\n");
        lc->release(ino);
        return IOERR;
    }
    size_t tmp = (size_t)(size + off);
    if (tmp > buf.length()) buf.resize(tmp, '\0');
    for (size_t i = off; i < tmp; i++) {
        buf[i] = data[i-off];
    }
    ec->log("begin");
    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("write - put error\n");
        lc->release(ino);
        return IOERR;
    }
    ec->log("end");
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    lc->acquire(parent);
    bool found = false;
    inum ino;
    std::list<dirent> dirent_list;
    std::string buf;

    if (!isdir(parent)) {
        lc->release(parent);
        return IOERR;
    }
    lookup(parent, name, found, ino);
    if (found) {
        ec->remove(ino);
        readdir(parent, dirent_list);
        for (std::list<dirent>::iterator it = dirent_list.begin(); it != dirent_list.end(); it++) {
            if (std::string(name) == it->name){
                dirent_list.erase(it);
                break;
            }
        }
    }
    for (std::list<dirent>::iterator it = dirent_list.begin(); it != dirent_list.end(); it++) {
        buf += std::string(it->name) + "//" + filename(it->inum) + "//";
    }
    ec->log("begin");
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("unlink - put error\n");
        lc->release(parent);
        return IOERR;
    }
    ec->log("end");
    lc->release(parent);
    return r;
}

void yfs_client::commit()
{
    ec->commit();
    return;
}

void yfs_client::undo()
{
    ec->undo();
    return;
}

void yfs_client::redo()
{
    ec->redo();
    return;
}
