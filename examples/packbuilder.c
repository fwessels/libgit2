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
	git_packbuilder_new(&_packbuilder, _repo);
}

void packbuilder_cleanup(void)
{
	git_packbuilder_free(_packbuilder);
	_packbuilder = NULL;

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

bool setup_packbuilder(git_oid *oid, int number)
{	
	int i;
	for (i = 0; i < number; i++) {
				
		/* Add commit to packbuilder */
		if (!index_cache_has_key(oid)) {
			git_oid *o = git__malloc(GIT_OID_RAWSZ); git_oid_cpy(o, oid);
			git_packbuilder_insert(_packbuilder, o, NULL);
		}
				
		git_commit *commit;
		git_commit_lookup(&commit, _repo, oid);

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
			
			git_commit *parent = NULL;
			if (git_commit_parent(&parent, commit, 0) < 0) {
				fprintf(stderr, "Failed to lookup parent\n");
				exit(-1);
			}
			git_oid_cpy(oid, git_commit_id(parent));
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

static int feed_indexer(void *ptr, size_t len, void *payload)
{
	git_transfer_progress *stats = (git_transfer_progress *)payload;

	return git_indexer_stream_add(_indexer, ptr, len, stats);
}

bool packbuilder_create_pack(git_oid *oid, int number)
{
	git_transfer_progress stats;
	char hex[41]; hex[40] = '\0';
	char index_file_to_add[1024];

	bool last_pack = setup_packbuilder(oid, number);

	git_indexer_stream_new(&_indexer, ".", NULL, NULL, NULL);
	git_packbuilder_foreach(_packbuilder, feed_indexer, &stats);
	git_indexer_stream_finalize(_indexer, &stats);

	git_oid_fmt(hex, git_indexer_stream_hash(_indexer));
	sprintf(index_file_to_add, "pack-%s.idx", hex);
	printf("adding to index: %s\n", index_file_to_add);
	add_to_index_cache(index_file_to_add);

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

	file = fopen(name, "rb");
	if (!file) {
		fprintf(stderr, "Unable to open file %s\n", name);
		return NULL;
	}
	
	fseek(file, 0, SEEK_END);
	fileLen = ftell(file);
	fseek(file, 0, SEEK_SET);

	buffer = (int *)malloc(fileLen);
	if (!buffer) {
		fprintf(stderr, "Unable to allocate memory\n");
        fclose(file);
		return NULL;
	}

	fread(buffer, fileLen, 1, file);
	fclose(file);

	// Check version 2 of index file (see https://www.kernel.org/pub/software/scm/git/docs/technical/pack-format.txt) 
	unsigned int magic   = get_big_endian_int(&buffer[0]);
	unsigned int version = get_big_endian_int(&buffer[1]);
	
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
		/* TODO: Remove hard limit on index cache */
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
  
	int i = 0;
	/* TODO: We could do a binary search here for performance optimization */
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

	fprintf(stderr, "/* TODO: Read initial set of .idx files into cache */\n");
	fprintf(stderr, "/* TODO: Verify extracted objects from generated pack file match with original loose object (and delete loose object) */\n");
	/* TODO: Make stepsize configurable or command line arguments */
	/* TODO: Optional: combine multiple pack files into single pack file */

	if (argc > 1)
		dir = argv[1];
	if (!dir || argc > 2) {
		fprintf(stderr, "usage: packbuilder [<repo-dir>]\n");
		return 1;
	}

	if (git_repository_open_ext(&_repo, dir, 0, NULL) < 0) {
		fprintf(stderr, "could not open repository: %s\n", dir);
		return 1;
	}
	
	git_reference *reference;
	if (git_repository_head(&reference, _repo) < 0) {
		fprintf(stderr, "could not get head reference\n");
		return 1;
	}
	
	git_oid oid;
	if (git_reference_name_to_id(&oid, _repo, "HEAD") < 0) {
		fprintf(stderr, "Failed to lookup HEAD reference and resolve to OID\n");
		return 1;
	}

	int i = 0;
	bool last_pack = false;
	#define STEP_SIZE 1000
	for (; /*i < 1000 &&*/ !last_pack; i += STEP_SIZE) {
		
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
