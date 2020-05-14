#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

#define T_DIR 1  // Directory
#define T_FILE 2 // File
#define T_DEV 3  // Special device
// On-disk file system format.
// Both the kernel and user programs use this header file.

// Block 0 is unused.
// Block 1 is super block.
// Inodes start at block 2.

#define ROOTINO 1 // root i-number
#define BSIZE 512 // block size

#define NDIRECT 12
#define NINDIRECT (BSIZE / sizeof(uint))

// Inodes per block.
#define IPB (BSIZE / sizeof(struct dinode))

#define BPB (BSIZE * 8)
// Directory is a file containing a sequence of dirent structures.
#define DIRSIZ 14

// File system super block
struct superblock
{
    uint size;    // Size of file system image (blocks)
    uint nblocks; // Number of data blocks
    uint ninodes; // Number of inodes.
};

// On-disk inode structure
struct dinode
{
    short type;              // File type
    short major;             // Major device number (T_DEV only)
    short minor;             // Minor device number (T_DEV only)
    short nlink;             // Number of links to inode in file system
    uint size;               // Size of file (bytes)
    uint addrs[NDIRECT + 1]; // Data block addresses
};

struct dirent
{
    ushort inum;
    char name[DIRSIZ];
};

int img;
void *img_ptr;

struct superblock *super_block;
struct dinode *start;
char *bitmap;
uint sbitmap_bytes;
uint bitmap_block;
int dircount;
struct dinode *lost_found;
void *free_data_block;

void format_check(int i, struct dirent **rootdir, struct dirent **parentdir)
{

    struct dinode *temp = &start[i];
    short temp_type = temp->type;
    int root_count = 0;
    int parent_count = 0;
    if (temp_type == T_DIR)
    {
        int cur_num = 0;
        while (cur_num < NDIRECT)
        {
            struct dirent *cur_dirent = img_ptr + (BSIZE * (temp->addrs[cur_num]));
            for (int b = 0; b < BSIZE / sizeof(struct dirent); b++)
            {
                if (!(cur_dirent->inum))
                {
                    cur_dirent++;
                    continue;
                }
                else
                {
                    if (strcmp("..", cur_dirent->name) == 0)
                    {
                        parent_count++;
                        *parentdir = cur_dirent;
                    }
                    else if (strcmp(cur_dirent->name, ".") == 0)
                    {
                        root_count++;
                        *rootdir = cur_dirent;
                    }
                }
                cur_dirent++;
            }
            cur_num++;
        }
        if (temp->addrs[NDIRECT])
        {
            uint *dir_entry = (uint *)(img_ptr + (temp->addrs[NDIRECT]) * BSIZE);

            int cur_num = 0;
            while (cur_num < NINDIRECT)
            {
                struct dirent *cur_dirent = (struct dirent *)(img_ptr + (BSIZE * (dir_entry[cur_num])));
                for (int c = 0; c < BSIZE / sizeof(struct dirent); c++)
                {
                    if (!(cur_dirent->inum))
                    {
                        cur_dirent++;
                        continue;
                    }
                    else
                    {
                        if (strcmp("..", cur_dirent->name) == 0)
                        {
                            parent_count++;
                            *parentdir = cur_dirent;
                        }
                        else if (strcmp(".", cur_dirent->name) == 0)
                        {
                            root_count++;
                            *rootdir = cur_dirent;
                        }
                    }

                    cur_dirent++;
                }
                cur_num++;
            }
        }
        if ( parent_count != 1 || root_count != 1 )
        {
            fprintf(stderr, "ERROR: directory not properly formatted.\n");
            exit(1);
        }
    }
}
void root_dir_check(int cur_node, short temp_type, struct dirent* rootdir,struct dirent* parentdir){
    if (cur_node == ROOTINO && temp_type == T_DIR)
        {
            if (rootdir->inum != parentdir->inum && (rootdir->inum == ROOTINO || parentdir->inum == ROOTINO))
            {
                fprintf(stderr, "ERROR: root directory does not exist.\n");
                exit(1);
            }
        }
        else if (cur_node == ROOTINO && temp_type != T_DIR)
        {
            fprintf(stderr, "ERROR: root directory does not exist.\n");
            exit(1);
        }
}
void bad_address_check(struct dinode* temp){
    int cur_num = 0;
            while(cur_num< NDIRECT)
            {
                if (temp->addrs[cur_num] < 0 || temp->addrs[cur_num] > 1023)
                {
                    fprintf(stderr, "ERROR: bad direct address in inode.\n");
                    exit(1);
                }
                
                cur_num ++;
            }

            

            int cur_num2 = 0;
            uint *entry = (uint *)(img_ptr + (temp->addrs[NDIRECT]) * BSIZE);
            while(cur_num2< NINDIRECT)
            {
                if (entry[cur_num2] > 1023 || entry[cur_num2] < 0 )
                {
                    fprintf(stderr, "ERROR: bad indirect address in inode.\n");
                    exit(1);
                }
                cur_num2 ++;
            }

}

void file_size_check(int cur_node){
    int cur_size = 0;
                if (start[cur_node].addrs[NDIRECT])
                {
                    uint *indir = (uint *)(img_ptr + start[cur_node].addrs[NDIRECT] * BSIZE);
                    for (int k = 0; k < BSIZE / sizeof(uint); k++)
                    {
                        if (indir[k] != 0)
                        {
                            cur_size++;
                        }
                    }
                }
                for (int j = 0; j < NDIRECT; j++)
                {
                    if (start[cur_node].addrs[j] != 0)
                    {
                        cur_size++;
                    }
                }

            
                if ((int)start[cur_node].size > cur_size * BSIZE || (int)start[cur_node].size <= (cur_size - 1) * BSIZE)
                {
                    fprintf(stderr, "ERROR: incorrect file size in inode.\n");
                    exit(1);
                }
}


void check_inodes()
{
    struct dinode *temp = start;
    struct dirent *rootdir;
    struct dirent *parentdir;

    int cur_node = 0; 
    while(cur_node < super_block->ninodes)
    {
        short temp_type = temp->type;
        
        if (temp_type > T_DEV || temp_type < 0 )
        {
            fprintf(stderr, "ERROR: bad inode.\n");
            exit(1);
        }

        format_check(cur_node, &rootdir, &parentdir);
        root_dir_check(cur_node, temp_type, rootdir, parentdir);
    
        if (temp_type != 0)
        {   
            bad_address_check(temp);
        }

        if (start[cur_node].type == T_DEV || start[cur_node].type == T_DIR || start[cur_node].type == T_FILE)
        {
            file_size_check(cur_node);
        }
        if (temp_type == T_DIR)
        {
            if (rootdir->inum != cur_node || strcmp(rootdir->name, ".") != 0 || strcmp(parentdir->name, "..") != 0)
            {
                fprintf(stderr, "ERROR: directory not properly formatted.\n");
                exit(1);
            }
        }
        temp++;
        cur_node ++;
    }
}

void check_marking(struct dinode* temp, int cur_num){
    if (temp->addrs[NDIRECT] > 0)
            {
               

                uint *entry = (uint *)(img_ptr + (temp->addrs[NDIRECT]) * BSIZE);
                cur_num = 0; 
                while(cur_num < NINDIRECT)
                {
                    uint index = entry[cur_num];
                    uint mask = 0x1 << (index % 8);
                    uint mark = bitmap[index / 8] & mask;
                    
                    if (index && !mark)
                    {
                        fprintf(stderr, "ERROR: address used by inode but marked free in bitmap.\n");
                        exit(1);
                    }
                    cur_num++; 
                }
            }

            cur_num = 0;
            while(cur_num < NDIRECT)
            {
                uint index = temp->addrs[cur_num];
                uint bitmask = 0x1 << (index % 8);
                uint mark = bitmap[index / 8] & bitmask;
                if (index && !mark)
                {
                    fprintf(stderr, "ERROR: address used by inode but marked free in bitmap.\n");
                    exit(1);
                }
                cur_num++;
            }
}

void check_more_than_once(struct dinode* temp2, char* indir_store, char* store){
      int cur = 0;
            uint index = temp2->addrs[cur];
            for (cur = 0; cur < NDIRECT + 1; cur++)
            {
                
                index = temp2->addrs[cur];
                if (store[index])
                {
                    fprintf(stderr, "ERROR: direct address used more than once.\n");
                    exit(1);
                }
                if (index)
                {
                    store[index]++;
                    if (cur == NDIRECT)
                    {
                        if (indir_store[index])
                        {
                            fprintf(stderr, "ERROR: indirect address used more than once.\n");
                            exit(1);
                        }
                        indir_store[index]++;
                    }
                }
                
            }
            if (temp2->addrs[NDIRECT] > 0)
            {
                uint *entry = (uint *)(img_ptr + (temp2->addrs[NDIRECT]) * BSIZE);
                for (cur = 0; cur < NINDIRECT; cur ++)
                {
                    uint index = entry[cur];
                    if (store[index])
                    {
                        fprintf(stderr, "ERROR: direct address used more than once.\n");
                        exit(1);
                    }
                    if (index)
                    {
                        if (indir_store[index])
                        {
                            fprintf(stderr, "ERROR: indirect address used more than once.\n");
                            exit(1);
                        }
                        indir_store[index]++;
                        store[index]++;
                    }
                }
            }
}

void check_bitmap()
{
    struct dinode *temp = start;
    int cur_num = 0;
    int cur_node = 0;
    
    while(cur_node < super_block->ninodes)
    {
        short temp_type = temp->type;
        if (temp_type)
        {   
            check_marking(temp, cur_num);
            
        }
        cur_node++; 
        temp++;
    }
    char store[1024] = {};

    cur_node = 0;
    struct dinode *temp2 = start;
    char indir_store[1024] = {};
    
    for (cur_node = 0; cur_node < super_block->ninodes; cur_node++)
    {
        short temp_type = temp2->type;
        if (temp_type)
        {
            check_more_than_once(temp2, indir_store, store);
        }
        temp2++;
    }

    cur_node = 0; 
    while (cur_node < bitmap_block + 1)
    {
        store[cur_node]++;
        cur_node++;
    }

    cur_node = 0;
    while ( cur_node < sbitmap_bytes)
    {
        uint cur_byte = bitmap[cur_node];
        for (cur_num = 0; cur_num < 8; cur_num++)
        {
            uint mask = 0x1 << cur_num;
            uint mark = cur_byte & mask;
            if (mark)
            {
                int index = cur_node * 8 + cur_num;
                int count = store[index];
                if (!count)
                {
                    fprintf(stderr, "ERROR: bitmap marks block in use but it is not in use.\n");
                    exit(1);
                }
            }
        }
        cur_node++;
    }
}

void check_data()
{
    struct dinode *temp = start;
    int cur_node;
    
    int *ref_dirent;
    int *ref_inode;
    int *ref_link;
    int *ref_dir;

    ref_link = (int *)calloc(super_block->ninodes, sizeof(int));
    ref_dirent = (int *)calloc(super_block->ninodes, sizeof(int));
    ref_inode = (int *)calloc(super_block->ninodes, sizeof(int));
    ref_dir = (int *)calloc(super_block->ninodes, sizeof(int));
    
    while(cur_node < super_block->ninodes)
    {
        if (temp->type)
        {
            ref_inode[cur_node] = 1;
            dircount += temp->nlink;
        }
        cur_node++;
        temp++;
    }
    temp = start;
    int cur = 0;
    for (cur_node = 0; cur_node < super_block->ninodes; cur_node++)
    {
        if (temp->type == T_DIR)
        {
            
            for (cur = 0; cur < NDIRECT; cur++)
            {
                struct dirent *data_entry = (struct dirent *)(img_ptr + BSIZE * (temp->addrs[cur]));

                int x = 0;
                while(x < BSIZE / sizeof(struct dirent))
                {
                    if (data_entry->inum != cur_node && data_entry->inum )
                    {
                        ref_dirent[data_entry->inum]++;
                        
                        if (start[data_entry->inum].type == T_DIR)
                        {
                            if (strcmp(data_entry->name, "..") != 0 && strcmp(data_entry->name, ".") != 0 )
                            {
                                ref_dir[data_entry->inum]++;
                            }
                        }
                        else if (start[data_entry->inum].type == T_FILE)
                        {
                            ref_link[data_entry->inum]++;
                        }
                    }
                    x++;
                    data_entry++;

                }
            }
            uint *entry = (uint *)(img_ptr + (temp->addrs[NDIRECT]) * BSIZE);
            for (cur = 0; cur < NINDIRECT; cur++)
            {
                struct dirent *data_entry = (struct dirent *)(img_ptr + BSIZE * (entry[cur]));
                int y = 0;
                while(y < BSIZE / sizeof(struct dirent))
                {
                    if (data_entry->inum && data_entry->inum != cur_node)
                    {
                        ref_dirent[data_entry->inum]++;
                        if (start[data_entry->inum].type == T_DIR)
                        {
                            if (strcmp(data_entry->name, ".") != 0 && strcmp(data_entry->name, "..") != 0)
                            {
                                ref_dir[data_entry->inum]++;
                            }
                        }
                        else if (start[data_entry->inum].type == T_FILE)
                        {
                            ref_link[data_entry->inum]++;
                        }
                        
                    }
                    y++;
                    data_entry++;
                }
            }
        }
        temp++;
    }
    ref_dirent[ROOTINO]++;
    for (cur_node = 0; cur_node < super_block->ninodes; cur_node++)
    {
        if (!ref_dirent[cur_node] && ref_inode[cur_node])
        {
            fprintf(stderr, "ERROR: inode marked used but not found in a directory.\n");
            exit(1);
        }
    }

    for (cur_node = 0; cur_node < super_block->ninodes; cur_node++)
    {
        if (ref_dir[cur_node] > 1)
        {
            fprintf(stderr, "ERROR: directory appears more than once in file system.\n");
            exit(1);
        }

        if (ref_link[cur_node] != start[cur_node].nlink && start[cur_node].type == T_FILE)
        {
            fprintf(stderr, "ERROR: bad reference count for file.\n");
            exit(1);
        }
      
        if (ref_dirent[cur_node] && !ref_inode[cur_node])
        {
            fprintf(stderr, "ERROR: inode referred to in directory but marked free.\n");
            exit(1);
        }
    }
    free(ref_inode);
    free(ref_link);
    free(ref_dir);
    free(ref_dirent);
    
}

int main(int argc, char *argv[])
{
    if (argc == 2)
    {
        char *image_name = argv[1];
        if ((img = open(image_name, O_RDONLY)) < 0)
        {
            fprintf(stderr, "image not found.\n");
            exit(1);
        }
        struct stat sbuf;
        fstat(img, &sbuf);
        if ((img_ptr = mmap(NULL, sbuf.st_size, PROT_READ, MAP_PRIVATE, img, 0)) < 0)
        {
            fprintf(stderr, "Failed to mmap\n");
            exit(1);
        }

        super_block = (struct superblock *)(img_ptr + BSIZE);
        start = (struct dinode *)(img_ptr + 2 * BSIZE);
        bitmap_block = super_block->ninodes / IPB + 3;
        bitmap = (char *)(img_ptr + (BSIZE * bitmap_block));
        sbitmap_bytes = 1024 / 8;
        dircount = 0;
        free_data_block = img_ptr + (BSIZE * (bitmap_block + 1));

        uint sb_size = super_block->size;
        if (sb_size < ((super_block->nblocks) / BPB + (super_block->nblocks) + bitmap_block))
        {
            fprintf(stderr, "ERROR: superblock is corrupted.\n");
            exit(1);
        }

        check_inodes();
        check_bitmap();
        check_data();
        exit(0);
    }
    else
    {
        fprintf(stderr, "Usage: xfsck <file_system_image>\n");
        exit(1);
    }
}