#include <git2.h>
#include <stdio.h>
#include <string.h>

#include "fileops.h"
#include "hash.h"
#include "iterator.h"
#include "vector.h"
#include "posix.h"

#define UNUSED(x) (void)(x)

static git_repository *_repo;
static git_revwalk *_revwalker;
static git_packbuilder *_packbuilder;
static git_indexer_stream *_indexer;

void packbuilder_initialize(void);
void packbuilder_cleanup(void);
bool setup_packbuilder(git_oid *oid, int number);
bool packbuilder_create_pack(git_oid *oid, int number);
unsigned int get_big_endian_int(void *pointer);
int * read_index(const char *name);
void add_to_index_cache(const char *name);
bool index_has_key(int *index, const git_oid *id);
bool index_cache_has_key(const git_oid *id);

void packbuilder_initialize(void)
{
	git_revwalk_new(&_revwalker, _repo);
	git_packbuilder_new(&_packbuilder, _repo);
}

void packbuilder_cleanup(void)
{
	git_packbuilder_free(_packbuilder);
	_packbuilder = NULL;

	git_revwalk_free(_revwalker);
	_revwalker = NULL;

	git_indexer_stream_free(_indexer);
	_indexer = NULL;
}

static int cb_tree_walk(const char *root, const git_tree_entry *entry, void *context)
{
	UNUSED(root);
	
	/* A commit inside a tree represents a submodule commit and should be skipped. */
	if (git_tree_entry_type(entry) == GIT_OBJ_COMMIT)
		return 0;

	/* We do not want any blobs in our pack files, so skip blobs */
	if (git_tree_entry_type(entry) == GIT_OBJ_BLOB)
		return 0;

    /* Already in an existing index file */
	if (index_cache_has_key(git_tree_entry_id(entry)))
		return 0;

	return git_packbuilder_insert((git_packbuilder *)context, git_tree_entry_id(entry), NULL);
}

/*
void walk_tree()
{
	git_oid oid;
	
	git_revwalk_sorting(_revwalker, GIT_SORT_TIME);
	git_revwalk_push_ref(_revwalker, "HEAD");
	
    while (git_revwalk_next(&oid, _revwalker) == 0) {
		char oidstr[GIT_OID_HEXSZ + 1];
		git_oid_tostr(oidstr, sizeof(oidstr), oid);
			  	printf("commit: %s\n", oidstr);
		
		// o = git__malloc(GIT_OID_RAWSZ);
		// git_oid_cpy(o, &oid);
		// cl_git_pass(git_vector_insert(&_commits, o));
    }
}
*/
bool setup_packbuilder(git_oid *oid, int number)
{	
	// git_revwalk_sorting(_revwalker, GIT_SORT_TIME);
	// git_revwalk_push_ref(_revwalker, "HEAD");

	// git_revwalk_sorting(_revwalker, /*GIT_SORT_TOPOLOGICAL |*/ GIT_SORT_REVERSE);
    // git_revwalk_sorting(_revwalker, GIT_SORT_TIME | GIT_SORT_REVERSE);
	// git_revwalk_push(_revwalker, &head_oid);

	int i;
	for (i = 0; i < number; i++ /*git_revwalk_next(&oid, _revwalker) == 0*/) {
				
		// char oidstr[GIT_OID_HEXSZ + 1];
		// git_oid_tostr(oidstr, sizeof(oidstr), oid);
		// 	  	printf("commit: %s\n", oidstr);

		/* Add commit to packbuilder */
		if (!index_cache_has_key(oid)) {
			git_oid *o = git__malloc(GIT_OID_RAWSZ); git_oid_cpy(o, oid);
			git_packbuilder_insert(_packbuilder, o, NULL);
		}
				
		git_commit *commit;
		git_commit_lookup(&commit, _repo, oid);

		// git_oid_tostr(oidstr, sizeof(oidstr), git_commit_tree_id(commit));
		// 	  	printf("  tree: %s\n", oidstr);
		
		/* Add tree to packbuilder */
		if (!index_cache_has_key(git_commit_tree_id(commit))) {
			git_oid *o = git__malloc(GIT_OID_RAWSZ); git_oid_cpy(o, git_commit_tree_id(commit));
			git_packbuilder_insert(_packbuilder, o, NULL);
		}
				
		git_tree *tree;
		git_tree_lookup(&tree, _repo, git_commit_tree_id(commit));

		/* Walk the tree to add subtrees to the packbuilder */
		git_tree_walk(tree, GIT_TREEWALK_PRE, cb_tree_walk, _packbuilder);
		git_tree_free(tree);
		
		/* Search parent commit */
		unsigned int parents = git_commit_parentcount(commit);
		
		if (parents > 1) {
			fprintf(stderr, "Found more than one parent for commit\n");
			exit(-1);
		}
		else if (parents == 1) {
			char out[41]; out[40] = '\0';
			
			// const git_oid *p_oid = git_commit_parent_id(commit, 0);
			// git_oid_fmt(out, p_oid);
			// printf("Parent: %s\n", out);
			
			git_commit *parent = NULL;
			int error;
			error = git_commit_parent(&parent, commit, 0);
			// printf("after git_commit_parent: error = %d, parent = %08x\n", error, (unsigned long)parent);
			// printf("before git_commit_id(parent)\n");
			const git_oid *parent_oid = git_commit_id(parent);
			// printf(" after git_commit_id(parent)\n");
			// git_oid_fmt(out, parent_oid);
			// printf("Parent: %s\n", out);
						
			git_oid_cpy(oid, parent_oid);
			git_commit_free(commit);
			git_commit_free(parent);
			continue;
		}
		else {
			/* No parent found, so this must be the first commit, break out */ 
			git_commit_free(commit);
			return true;
		}
	}
	return false;
}

// static int feed_indexer_count = 0;

static int feed_indexer(void *ptr, size_t len, void *payload)
{
	// feed_indexer_count++;
	// if (feed_indexer_count % 1000 == 0) fprintf(stderr, "feed_indexder: %d\n", feed_indexer_count);
	git_transfer_progress *stats = (git_transfer_progress *)payload;

	return git_indexer_stream_add(_indexer, ptr, len, stats);
}

bool packbuilder_create_pack(git_oid *oid, int number)
{
	git_transfer_progress stats;
	char hex[41]; hex[40] = '\0';
	char index_file_to_add[1024];

	bool last_pack = setup_packbuilder(oid, number);

	git_indexer_stream_new(&_indexer, ".", NULL, NULL);
	git_packbuilder_foreach(_packbuilder, feed_indexer, &stats);
	git_indexer_stream_finalize(_indexer, &stats);

	git_oid_fmt(hex, git_indexer_stream_hash(_indexer));
	sprintf(index_file_to_add, "pack-%s.idx", hex);
	printf("adding to index: %s\n", index_file_to_add);
	add_to_index_cache(index_file_to_add);

	// git_oid oid_search;
	//     git_oid_fromstr(&oid_search, "0d42b46341e49da5ef456b09c17ae2081fcd49b9");
	// 
	// bool found = index_cache_has_key(&oid_search);
	// printf("%s", found ? "yes" : "no");

	return last_pack;
}

unsigned int get_big_endian_int(void *pointer)
{
	return (((unsigned char*)pointer)[0] << 24) + (((unsigned char*)pointer)[1] << 16) + (((unsigned char*)pointer)[2] << 8) + (((unsigned char*)pointer)[3]);
}

int * read_index(const char *name)
{
	FILE *file;
	int *buffer;
	unsigned long fileLen;

	// Open file
	file = fopen(name, "rb");
	if (!file) {
		fprintf(stderr, "Unable to open file %s\n", name);
		return NULL;
	}
	
	// Get file length
	fseek(file, 0, SEEK_END);
	fileLen = ftell(file);
	fseek(file, 0, SEEK_SET);

	// Allocate memory
	buffer = (int *)malloc(fileLen);
	if (!buffer) {
		fprintf(stderr, "Unable to allocate memory\n");
        fclose(file);
		return NULL;
	}

	// Read file contents into buffer
	fread(buffer, fileLen, 1, file);
	fclose(file);

	// Check version 2 of index file (see https://www.kernel.org/pub/software/scm/git/docs/technical/pack-format.txt) 
	unsigned int magic   = get_big_endian_int(&buffer[0]);
	unsigned int version = get_big_endian_int(&buffer[1]);
	
	int * index = (int*) &buffer[2];
	int i, n, nr;
	nr = 0;
	for (i = 0; i < 256; i++) {
			uint32_t n = ntohl(index[i]);
			printf("n from index = %d\n", n);
			if (n < nr) {
				printf("Failed to check index. Index is non-monotonic");
			}
			nr = n;
	}
	
	if (magic != 0xff744f63 || version != 2) {
		fprintf(stderr, "Bad magic (%08x) or bad version (%d)\n", magic, version);
		free(buffer);
		return NULL;
	}
	
	return buffer;
}

static int index_cache_entries = 0;
#define INDEX_SIZE 4096
static int * index_cache[INDEX_SIZE];

void add_to_index_cache(const char *name)
{
	if (index_cache_entries == INDEX_SIZE - 1) {
		/* Remove hard limit on index cache */
		fprintf(stderr, "Index size too small -- aborting!\n");
		exit(-1);
	}
	int *index = read_index(name);
	index_cache[index_cache_entries++] = index;
}

bool index_cache_has_key(const git_oid *id)
{
	int i = 0;
	for (; i < index_cache_entries; i++) {
		if (index_has_key(index_cache[i], id)) return true;
	}
	return false;
}

bool index_has_key(int *index, const git_oid *id)
{
	int fanout_255 = get_big_endian_int(&index[2 + 255]);
	// printf("fanout[255] = %d\n", fanout_255);
  
	int i = 0;
	/* We could do a binary search here for performance optimization */
	for (; i < fanout_255; i++) {
		
		void *oid = &index[2 + 256 + i * (20 / 4)];
		
		if (git_oid_cmp(oid, id) == 0) {
			return true;
		}
	}
	
	return false;
}

int main (int argc, char** argv)
{
	char *dir = ".";

	git_threads_init();

	if (argc > 1)
		dir = argv[1];
	if (!dir || argc > 2) {
		fprintf(stderr, "usage: packbuilder [<repo-dir>]\n");
		return 1;
	}

	if (git_repository_open_ext(&_repo, dir, 0 /*GIT_REPOSITORY_OPEN_BARE*/, NULL) < 0) {
		fprintf(stderr, "could not open repository: %s\n", dir);
		return 1;
	}
	
	git_oid oid_search;
    git_oid_fromstr(&oid_search, "c8832a1cce1f663aef958c6b48ec9e1677bc83ab");

	add_to_index_cache("/Users/frankw/rails_app/libgit2/examples/entries1000/pack-3e883c49486c4c68e8ba1d2b6d95bdb00dd4fff6.idx");
//	add_to_index_cache("/Users/frankw/rails_app/libgit2/examples/entries1000/pack-000d33de66ebef30c32f1c157efd100f42e1ab3c.idx");
	bool found = index_cache_has_key(&oid_search);
	printf("%s", found ? "yes" : "no");
	return -1;

	git_oid oid;
    git_oid_fromstr(&oid, "14e89d2dede53af2c76ea85e014ab832e95a4323");

	int i = 0;
	bool last_pack = false;
	#define STEP_SIZE 500
	for (; /*i < 5000 &&*/ !last_pack; i += STEP_SIZE) {
		
		char oidstr[GIT_OID_HEXSZ + 1];
		git_oid_tostr(oidstr, sizeof(oidstr), &oid);
		
		printf("doing commits: [%d..%d]; first commit id = %s\n", i, i +STEP_SIZE - 1, oidstr);
		packbuilder_initialize();
		last_pack = packbuilder_create_pack(&oid, STEP_SIZE);
		packbuilder_cleanup();
	}
	
	git_repository_free(_repo);
	_repo = NULL;

	git_threads_shutdown();

	return 0;
}
