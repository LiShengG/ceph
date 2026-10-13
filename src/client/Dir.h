#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

#include <string>
#include <unordered_map>
#include <vector>

class Dentry;
struct Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  std::unordered_map<std::string, Dentry*> dentries;
  unsigned num_null_dentries = 0;

  std::vector<Dentry*> readdir_cache;

  // Shared streams cover [0, end) while the directory generation matches.
  // Matching ordered_count also makes readdir_cache usable in that order.
  // Hash-ordered and frag-ordered replies cannot share a pass.
  struct readdir_pass_t {
    // Unique across the client, including closed and reopened directories.
    uint64_t id = 0;
    bool active = false;
    bool hash_order = false;
    uint64_t release_count = 0;
    uint64_t ordered_count = 0;
    int shared_gen = 0;
    int64_t end = 0;
    // Frag boundaries record only the next hash, independent of split bits.
    bool end_at_frag_boundary = false;
  } readdir_pass;

  explicit Dir(Inode* in) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

#endif
