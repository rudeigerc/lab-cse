#include <pthread.h>



#include "inode_manager.h"
#include <unistd.h>
#include <fstream>

// disk layer -----------------------------------------

disk::disk()
{
  pthread_t id;
  int ret;
  bzero(blocks, sizeof(blocks));

  ret = pthread_create(&id, NULL, test_daemon, (void*)blocks);
  if(ret != 0)
	  printf("FILE %s line %d:Create pthread error\n", __FILE__, __LINE__);
}

void disk::read_block(blockid_t id, char *buf)
{
	/*
	*your lab1 code goes here.
	*if id is smaller than 0 or larger than BLOCK_NUM
	*or buf is null, just return.
	*put the content of target block into buf.
	*hint: use memcpy
	*/
	if (id < 0 || id > BLOCK_NUM || buf == NULL) return;
	memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
	/*
	*your lab1 code goes here.
	*hint: just like read_block
	*/
	if (id < 0 || id > BLOCK_NUM || buf == NULL) return;
	memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block()
{
	/*
	* your lab1 code goes here.
	* note: you should mark the corresponding bit in block bitmap when alloc.
	* you need to think about which block you can start to be allocated.

	* hint: use macro IBLOCK and BBLOCK.
			use bit operation.
			remind yourself of the layout of disk.
	*/
	blockid_t id = IBLOCK(INODE_NUM, BLOCK_NUM);
	char tmp[BLOCK_SIZE];
	while (id < BLOCK_NUM) {
		d->read_block(BBLOCK(id), tmp);
		uint32_t num = id % BLOCK_SIZE;
		uint32_t* buf = &((uint32_t *)tmp)[num/8];
		if (!(*buf & (1 << num))) {
			*buf |= (1 << num);
			d->write_block(BBLOCK(id), tmp);
			break;
		}
		id++;
	}
	return id;
}

void block_manager::free_block(uint32_t id)
{
	/*
	* your lab1 code goes here.
	* note: you should unmark the corresponding bit in the block bitmap when free.
	*/
    char tmp[BLOCK_SIZE];
    d->read_block(BBLOCK(id), tmp);
    uint32_t num = id % BLOCK_SIZE;
    uint32_t* buf = &((uint32_t *)tmp)[num/8];
    *buf &= ~(1 << num);
    d->write_block(BBLOCK(id), tmp);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
	d = new disk();

	// format the disk
	sb.size = BLOCK_SIZE * BLOCK_NUM;
	sb.nblocks = BLOCK_NUM;
	sb.ninodes = INODE_NUM;

}

// reference: https://en.wikipedia.org/wiki/Hamming_code#[7,4]_Hamming_code_with_an_additional_parity_bit
char parity(char x)
{
    char res;
    res = x ^ (x >> 4);
    res = res ^ (res >> 2);
    res = res ^ (res >> 1);
    return res & 1;
}

char encode(char x)
{
    char res = x & 0x07; // P_5 P_6 P_7
    res |= (x & 0x08) << 1; // P_3
    res |= parity(res & 0x15) << 6; // P_1 <- P_3 ^ P_5 ^ P_7
    res |= parity(res & 0x13) << 5; // P_2 <- P_3 ^ P_6 ^ P_7
    res |= parity(res & 0x07) << 3; // P_4 <- P_5 ^ P_6 ^ P_7
    res = res << 1 | parity(res); // P_8
    return res;
}

char decode(char x, char &res)
{
    bool flag = false;
    char code = x >> 1;
    int fix = 0;
    if (parity(code & 0x55)) fix += 1;
    if (parity(code & 0x33)) fix += 2;
    if (parity(code & 0x0F)) fix += 4;
    if (fix) {
        flag = true;
        code ^= 1 << (7 - fix);
    }

    if (flag && !parity(x)) {
        return false;
    }

    res = (code & 0x07) | ((code & 0x10) >> 1);
    return true;
}

void block_manager::read_block(uint32_t id, char *buf)
{
    char block[BLOCK_SIZE * 4];

    d->read_block(id * 4, block);
    d->read_block(id * 4 + 1, block + BLOCK_SIZE);
    d->read_block(id * 4 + 2, block + BLOCK_SIZE * 2);
    d->read_block(id * 4 + 3, block + BLOCK_SIZE * 3);

    for (int i = 0; i < BLOCK_SIZE; i++) {
        char low, high;
        if (!decode(block[i * 2], low)) decode(block[i * 2 + BLOCK_SIZE * 2], low);
        if (!decode(block[i * 2 + 1], high)) decode(block[i * 2 + 1 + BLOCK_SIZE * 2], high);
        buf[i] = (high << 4) | low;
    }

	write_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
	char block[BLOCK_SIZE * 4];

    for (int i = 0; i < BLOCK_SIZE; i++) {
        char low = encode(buf[i] & 0x0F);
        char high = encode((buf[i] >> 4) & 0x0F);
        block[i * 2] = low;
        block[i * 2 + 1] = high;
        block[i * 2 + BLOCK_SIZE * 2] = low;
        block[i * 2 + 1 + BLOCK_SIZE * 2] = high;
    }

    d->write_block(id * 4, block);
    d->write_block(id * 4 + 1, block + BLOCK_SIZE);
    d->write_block(id * 4 + 2, block + BLOCK_SIZE * 2);
    d->write_block(id * 4 + 3, block + BLOCK_SIZE * 3);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
	bm = new block_manager();

    memset(alloc_table, 0, INODE_NUM);
    t_num = 1;
    version = 0;
    log_pos = 0;
    log_file.reserve(10);
    pthread_mutex_init(&log_lock, 0);

	uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
	if (root_dir != 1) {
		printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
		exit(0);
	}
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type)
{
	/*
	* your lab1 code goes here.
	* note: the normal inode block should begin from the 2nd inode block.
	* the 1st is used for root_dir, see inode_manager::inode_manager().

	* if you get some heap memory, do not forget to free it.
	*/
    static uint32_t inum = 1;
    while(alloc_table[inum]){
        inum = inum % (INODE_NUM - 1) + 1;
    }
    alloc_table[inum] = 1;

    char buf[BLOCK_SIZE];
    bm->read_block(IBLOCK(inum, BLOCK_NUM), buf);
    struct inode *ino = (struct inode*)(buf + inum % IPB);
    ino->type = type;
    ino->size = 0;
    ino->atime = time(0);
    ino->mtime = time(0);
    ino->ctime = time(0);
    bm->write_block(IBLOCK(inum, BLOCK_NUM), buf);
    return inum;
}

void inode_manager::free_inode(uint32_t inum)
{
	/*
	* your lab1 code goes here.
	* note: you need to check if the inode is already a freed one;
	* if not, clear it, and remember to write back to disk.
	* do not forget to free memory if necessary.
	*/

    alloc_table[inum] = 0;

}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* inode_manager::get_inode(uint32_t inum)
{
	struct inode *ino, *ino_disk;
	char buf[BLOCK_SIZE];

	printf("\tim: get_inode %d\n", inum);

	if (inum < 0 || inum >= INODE_NUM) {
		printf("\tim: inum out of range\n");
		return NULL;
	}

	bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
	// printf("%s:%d\n", __FILE__, __LINE__);

	ino_disk = (struct inode*)buf + inum%IPB;
	if (ino_disk->type == 0) {
		printf("\tim: inode not exist\n");
		return NULL;
	}

	ino = (struct inode*)malloc(sizeof(struct inode));
	*ino = *ino_disk;

	return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
	char buf[BLOCK_SIZE];
	struct inode *ino_disk;

	printf("\tim: put_inode %d\n", inum);
	if (ino == NULL)
		return;

	bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
	ino_disk = (struct inode*)buf + inum%IPB;
	*ino_disk = *ino;
	bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
	/*
	* your lab1 code goes here.
	* note: read blocks related to inode number inum,
	* and copy them to buf_out
	*/
	struct inode *ino = get_inode(inum);
	if (!ino) return;
    ino->atime = time(0);
	*size = ino->size;
	uint32_t nblocks = 0;

	// nblocks = ⌈(*size) / BLOCK_SIZE⌉
	if (*size) nblocks = (*size) / BLOCK_SIZE + !((*size) % BLOCK_SIZE == 0);
	*buf_out = (char *)malloc(nblocks * BLOCK_SIZE);

	for (uint32_t i = 0; i < MIN(nblocks, NDIRECT); i++) {
		bm->read_block(ino->blocks[i], *buf_out + i * BLOCK_SIZE);
	}

	if (nblocks > NDIRECT) {
		blockid_t tmp[NINDIRECT];
		bm->read_block(ino->blocks[NDIRECT], (char *)tmp);
		for (uint32_t i = 0; i < nblocks - NDIRECT; i++) {
			bm->read_block(tmp[i], *buf_out + NDIRECT * BLOCK_SIZE + i * BLOCK_SIZE);
		}
	}
	free(ino);
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
	/*
	* your lab1 code goes here.
	* note: write buf to blocks of inode inum.
	* you need to consider the situation when the size of buf
	* is larger or smaller than the size of original inode.
	* you should free some blocks if necessary.
	*/
	struct inode *ino = get_inode(inum);
	if (!ino) return;

	uint32_t noldBlocks = 0;
	uint32_t nnewBlocks = 0;

	// noldBlocks = ⌈(ino->size) / BLOCK_SIZE⌉
	if (ino->size) noldBlocks = (ino->size) / BLOCK_SIZE + !((ino->size) % BLOCK_SIZE == 0);
	// nnewBlocks = ⌈size / BLOCK_SIZE⌉
	if (size) nnewBlocks = size / BLOCK_SIZE + !(size % BLOCK_SIZE == 0);

	if (nnewBlocks <= noldBlocks) {
		// nnewBlocks <= noldBlocks
		// free useless blocks
		if (noldBlocks <= NDIRECT) {
			// nnewBlocks <= noldBlocks <= NDIRECT
			for (uint32_t i = nnewBlocks; i < noldBlocks; i++) {
				bm->free_block(ino->blocks[i]);
			}
		} else if (nnewBlocks > NDIRECT) {
			// NDIRECT < nnewBlocks <= noldBlocks
			blockid_t tmp[NINDIRECT];
			bm->read_block(ino->blocks[NDIRECT], (char *)tmp);
			for (uint32_t i = nnewBlocks; i < noldBlocks; i++) {
				bm->free_block(tmp[i-NDIRECT]);
			}
		} else {
			// nnewBlocks < NDIRECT < noldBlocks
			for (uint32_t i = nnewBlocks; i < NDIRECT; i++) {
				bm->free_block(ino->blocks[i]);
			}
			blockid_t tmp[NINDIRECT];
			bm->read_block(ino->blocks[NDIRECT], (char *)tmp);
			for (uint32_t i = 0; i < noldBlocks - NDIRECT; i++) {
				bm->free_block(tmp[i]);
			}
			bm->free_block(ino->blocks[NDIRECT]);
		}
	} else {
		// nnewBlocks > noldBlocks
		// alloc new blocks
		if (nnewBlocks <= NDIRECT) {
			// noldBlocks <= nnewBlocks <= NDIRECT
			for (uint32_t i = noldBlocks; i < nnewBlocks; i++) {
				ino->blocks[i] = bm->alloc_block();
			}
		} else if (noldBlocks > NDIRECT) {
			// NDIRECT < noldBlocks <= nnewBlocks
			blockid_t tmp[NINDIRECT];
			bm->read_block(ino->blocks[NDIRECT], (char *)tmp);
			for (uint32_t i = noldBlocks; i < nnewBlocks; i++) {
				tmp[i-NDIRECT] = bm->alloc_block();
			}
			bm->write_block(ino->blocks[NDIRECT], (char *)tmp);
		} else {
			// noldBlocks < NDIRECT < nnewBlocks
			for (uint32_t i = noldBlocks; i < NDIRECT; i++) {
				ino->blocks[i] = bm->alloc_block();
			}
			blockid_t tmp[NINDIRECT];
			ino->blocks[NDIRECT] = bm->alloc_block();
			for (uint32_t i = 0; i < nnewBlocks - NDIRECT; i++) {
				tmp[i] = bm->alloc_block();
			}
			bm->write_block(ino->blocks[NDIRECT], (char *)tmp);
		}
	}

    char tmp[BLOCK_SIZE];
    char _tmp[BLOCK_SIZE];
    uint32_t ptr = 0;
    for (uint32_t i = 0; i < NDIRECT && ptr < size; i++) {
        if (size - ptr > BLOCK_SIZE) {
            bm->write_block(ino->blocks[i], buf + ptr);
            ptr += BLOCK_SIZE;
        } else {
            uint32_t len = size - ptr;
            memcpy(tmp, buf + ptr, len);
            bm->write_block(ino->blocks[i], tmp);
            ptr += len;
        }
    }
    if (ptr < size) {
        bm->read_block(ino->blocks[NDIRECT], _tmp);
        for (uint32_t i = 0; i < NINDIRECT && ptr < size; i++) {
            blockid_t id = *((blockid_t *)_tmp + i);
            if (size - ptr > BLOCK_SIZE) {
                bm->write_block(id, buf + ptr);
                ptr += BLOCK_SIZE;
            } else {
                uint32_t len = size - ptr;
                memcpy(tmp, buf + ptr, len);
                bm->write_block(id, tmp);
                ptr += len;
            }
        }
    }

    ino->size = size;
    ino->mtime = time(0);
    ino->ctime = time(0);
	put_inode(inum, ino);
	free(ino);
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
	/*
	* your lab1 code goes here.
	* note: get the attributes of inode inum.
	* you can refer to "struct attr" in extent_protocol.h
	*/
	struct inode *ino = get_inode(inum);
	if (ino) {
		a.type = ino->type;
		a.atime = ino->atime;
		a.mtime = ino->mtime;
		a.ctime = ino->ctime;
		a.size = ino->size;
        free(ino);
	}
}

void inode_manager::remove_file(uint32_t inum)
{
	/*
	* your lab1 code goes here
	* note: you need to consider about both the data block and inode of the file
	* do not forget to free memory if necessary.
	*/
	struct inode *ino = get_inode(inum);
	uint32_t nblocks = 0;

	// nblocks = ⌈(ino->size) / BLOCK_SIZE⌉
	if (ino->size) nblocks = (ino->size) / BLOCK_SIZE + !((ino->size) % BLOCK_SIZE == 0);

	for (uint32_t i = 0; i < MIN(nblocks, NDIRECT); i++){
		bm->free_block(ino->blocks[i]);
	}

	if (nblocks > NDIRECT) {
		blockid_t tmp[NINDIRECT];
		bm->read_block(ino->blocks[NDIRECT], (char *)tmp);
		for (uint32_t i = 0; i < nblocks - NDIRECT; i++) {
			bm->free_block(tmp[i]);
		}
		bm->free_block(ino->blocks[NDIRECT]);
	}
	free_inode(inum);
	free(ino);
}

void inode_manager::log(std::string log_str, uint32_t& num){
    pthread_mutex_lock(&log_lock);
    std::stringstream ss;
    // begin
    if (log_str == "begin") {
        num = t_num++;
        ss << num << ' ' << log_str;
        log_file.push_back(ss.str());
    } else {
        //  put & remove
        ss << log_str;
        uint32_t _num;
        std::string inst;
        ss >> _num >> inst;
        if (inst == "put" || inst == "remove") {
            extent_protocol::extentid_t id;
            ss >> id;
            char *buf_out = NULL;
            int size;

            read_file(id, &buf_out, &size);
            std::string buf;
            buf.assign(buf_out, size);
            std::stringstream _ss;
            _ss << log_str << ' ' << size << ' ' << buf;
            log_str = _ss.str();
        } else if (inst == "end") {
            // end
            int i;
            int n = log_file.size();

            for (i = n - 1; i >= 0; i--){
                std::stringstream _ss;
                uint32_t _inst_num;
                std::string _inst;
                _ss << log_file[i];
                _ss >> _inst_num >> _inst;
                if(_inst == "begin" && _inst_num == _num)
                    break;
            }

            for (i = i + 1; i < n; i++){
                std::stringstream _ss;
                uint32_t _inst_num;
                std::string _inst;
                _ss << log_file[i];
                _ss >> _inst_num >> _inst;
                if (_inst_num == _num){
                    extent_protocol::extentid_t id;
                    if (_inst == "put"){
                        uint32_t size;
                        _ss >> id >> size;
                        _ss.get();
                        char c_buf[size];

                        for(uint32_t j=0; j<size; j++)
                            _ss.get(c_buf[j]);
                        write_file(id, c_buf, size);
                    }
                    else if(_inst == "remove"){
                        _ss >> id;
                        remove_file(id);
                    }
                }
            }
        }
        log_file.push_back(log_str);
    }
    pthread_mutex_unlock(&log_lock);
}

void inode_manager::commit(){
    std::stringstream ss;
    ss << t_num << " commit " << version++;
    log_file.push_back(ss.str());
}

void inode_manager::undo(){
    pthread_mutex_lock(&log_lock);
    uint32_t n = log_file.size();
    if (log_pos == 0) {
        log_pos = n - 1;
    }

    while (log_pos >= 0) {
        uint32_t tnum, vnum;
        std::string inst;
        std::stringstream ss;

        ss << log_file[log_pos];
        ss >> tnum >> inst;
        // commit
        if (inst == "commit") {
            ss >> vnum;
            if(vnum == version-1){
                version--;
                break;
            }
        } else if (inst == "create"){
            // create
            extent_protocol::extentid_t id;
            ss >> id;
            remove_file(id);
        } else if (inst == "put"){
            // put
            extent_protocol::extentid_t id;
            uint32_t size;
            ss >> id >> size;
            for (uint32_t j = 0; j < size + 2; j++) ss.get();
            ss >> size;
            ss.get();
            char c_buf[size];
            for (uint32_t j = 0; j < size; j++) ss.get(c_buf[j]);
            write_file(id, c_buf, size);
        } else if (inst == "remove"){
            // remove
            extent_protocol::extentid_t id;
            uint32_t size;
            ss >> id >> size;
            ss.get();
            char c_buf[size];
            for (uint32_t j = 0; j < size; j++) ss.get(c_buf[j]);
            alloc_table[id] = 1;
            write_file(id, c_buf, size);
        }
        log_pos--;
    }
    pthread_mutex_unlock(&log_lock);
}

void inode_manager::redo(){
    pthread_mutex_lock(&log_lock);
    uint32_t n = log_file.size();

    while (log_pos < n) {
        std::stringstream ss;
        uint32_t tnum, vnum;
        std::string inst;
        ss << log_file[log_pos];
        ss >> tnum >> inst;
        if (inst == "commit") {
            // commit
            ss >> vnum;
            if (vnum == version + 1){
                version++;
                break;
            }
        } else if (inst == "create") {
            // create
            extent_protocol::extentid_t id;
            ss >> id;
            alloc_table[id] = 1;

            write_file(id, "", 0);
        } else if (inst == "put") {
            // put
            extent_protocol::extentid_t id;
            uint32_t size;
            std::string buf;
            ss >> id >> size;
            ss.get();
            char c_buf[size];
            for (uint32_t j = 0; j < size; j++) ss.get(c_buf[j]);

            write_file(id, c_buf, size);
        } else if (inst == "remove") {
            // remove
            extent_protocol::extentid_t id;
            ss >> id;

            remove_file(id);
        }
        log_pos++;
    }
    pthread_mutex_unlock(&log_lock);
}
