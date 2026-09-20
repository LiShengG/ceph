#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

struct Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  ceph::unordered_map<string, Dentry*> dentries;
  unsigned num_null_dentries = 0;

  vector<Dentry*> readdir_cache;

  // A pass filling readdir_cache may span several dir_result_t, e.g. an NFS
  // server opening one per READDIR request.  While the parent inode's
  // counters match the snapshot taken when the pass started at the beginning
  // of the directory, the pass has seen every dentry with an offset below
  // 'end', and readdir_cache holds them in order if ordered_count matches too.
  // Offsets in hash order and in frag order do not compare, so a pass only
  // goes on with replies in the order it started with.
  struct readdir_pass_t {
    // unique among all passes of the client, see
    // dir_result_t::next_offset_pass
    uint64_t id = 0;
    bool active = false;
    bool hash_order = false;
    uint64_t release_count = 0;
    uint64_t ordered_count = 0;
    int shared_gen = 0;
    int64_t end = 0;
  } readdir_pass;

  explicit Dir(Inode* in) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

#endif
