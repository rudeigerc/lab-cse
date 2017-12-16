#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  	bzero(blocks, sizeof(blocks));
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

void block_manager::read_block(uint32_t id, char *buf)
{
	d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
	d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
	bm = new block_manager();
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
	uint32_t inum = 1;
	while (inum < INODE_NUM) {
		struct inode *ino = get_inode(inum);
		if (ino) {
			free(ino);
			inum++;
		} else {
			struct inode buf;
			buf.type = type;
			buf.atime = time(0);
			buf.mtime = time(0);
			buf.ctime = time(0);
			buf.size = 0;
			put_inode(inum, &buf);
			return inum;
		}
	}
	return 0;
}

void inode_manager::free_inode(uint32_t inum)
{
	/*
	* your lab1 code goes here.
	* note: you need to check if the inode is already a freed one;
	* if not, clear it, and remember to write back to disk.
	* do not forget to free memory if necessary.
	*/
	struct inode *ino = get_inode(inum);
	if (ino) {
		ino->type = 0;
		ino->atime = time(0);
		ino->mtime = time(0);
		ino->size = 0;
		put_inode(inum, ino);
	}
	free(ino);
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
    ino->mtime = time(0);
    ino->ctime = time(0);
	uint32_t noldBlocks = 0;
	uint32_t nnewBlocks = 0;

	// noldBlocks = ⌈(ino->size) / BLOCK_SIZE⌉
	if (ino->size) noldBlocks = (ino->size) / BLOCK_SIZE + !((ino->size) % BLOCK_SIZE == 0);
	// nnewBlocks = ⌈size / BLOCK_SIZE⌉
	if (size) nnewBlocks = size / BLOCK_SIZE + !(size % BLOCK_SIZE == 0);
	ino->size = size;

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
			bm->write_block(ino->blocks[NDIRECT], (char *)tmp);
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

	// write blocks
	for (uint32_t i = 0; i < MIN(nnewBlocks, NDIRECT); i++) {
		bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
	}

	if (nnewBlocks > NDIRECT) {
		blockid_t tmp[NINDIRECT];
		bm->read_block(ino->blocks[NDIRECT], (char *)tmp);
		for(uint32_t i = 0; i < nnewBlocks - NDIRECT; i++){
			bm->write_block(tmp[i], buf + (NDIRECT + i) * BLOCK_SIZE);
		}
	}
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
	}
	free(ino);
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
