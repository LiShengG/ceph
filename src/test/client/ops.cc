// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <errno.h>
#include "TestClient.h"
#include "client/Dentry.h"
#include "client/Dir.h"
#include "client/Inode.h"
#include "client/MetaRequest.h"
#include "client/MetaSession.h"
#include "include/scope_guard.h"
#include "mds/cephfs_features.h"
#include "messages/MClientReply.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock-more-matchers.h"

TEST_F(TestClient, CheckDummyOP) {
  ASSERT_EQ(client->check_dummy_op(myperm), -EOPNOTSUPP);
}

TEST_F(TestClient, CheckUnknownSessionOp) {
  ASSERT_EQ(client->send_unknown_session_op(-1), 0);
  sleep(5);
  ASSERT_EQ(client->check_client_blocklisted(), true);
}

TEST_F(TestClient, CheckZeroReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(0), true);
}
TEST_F(TestClient, CheckUnknownReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(2), true);
}
TEST_F(TestClient, CheckNegativeReclaimFlagUnmasked) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(-1 & ~MClientReclaim::FLAG_FINISH), true);
}
TEST_F(TestClient, CheckNegativeReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(-1), true);
}

TEST_F(TestClient, ReaddirRstatReplyProvenance) {
  std::scoped_lock lock(client->client_lock);
  MetaSession auth(0, {}, {});
  MetaSession replica(1, {}, {});
  InodeStat st;
  // A synthetic inode keeps the fabricated replies separate from the
  // mounted filesystem, and its caps from the real MDS sessions.
  st.vino = vinodeno_t((1ULL << 63) | static_cast<uint64_t>(getpid()), CEPH_NOSNAP);
  st.version = 2;
  st.mode = S_IFDIR | 0755;
  st.nlink = 1;
  st.dirstat.nfiles = 1;
  st.rstat.rbytes = 4096;
  st.inline_version = CEPH_INLINE_NONE;
  st.dir_pin = -1;
  memset(&st.cap, 0, sizeof(st.cap));
  memset(&st.dir_layout, 0, sizeof(st.dir_layout));

  // A newly encountered replica supplies an initial value, but no proof
  // that this listing has fetched rstat from the authoritative MDS.
  InodeRef in(client->add_update_inode(&st, {}, &replica, myperm, 20));
  // put_inode() only queues the release: drop the caps while the sessions
  // they refer to still exist.
  auto drop_caps = make_scope_guard([&] { client->remove_all_caps(in.get()); });
  EXPECT_EQ(4096, in->rstat.rbytes);
  EXPECT_EQ(0u, in->rstat_seq);

  st.cap.flags = CEPH_CAP_FLAG_AUTH;
  st.version = 4;
  st.rstat.rbytes = 8192;
  ASSERT_EQ(in.get(), client->add_update_inode(&st, {}, &auth, myperm, 21));
  EXPECT_EQ(8192, in->rstat.rbytes);
  EXPECT_EQ(21u, in->rstat_seq);

  // A reply from a newer request to a replica must not make its stale
  // value eligible for the listing that started at sequence 22.
  st.cap.flags = 0;
  st.version = 6;
  st.rstat.rbytes = 1024;
  ASSERT_EQ(in.get(), client->add_update_inode(&st, {}, &replica, myperm, 22));
  EXPECT_EQ(8192, in->rstat.rbytes);
  EXPECT_EQ(21u, in->rstat_seq);

  // An authoritative reply replaces rstat even without a newer inode
  // version, and an older request can arrive later: retain that request's
  // sequence so a listing refreshes the value.
  st.cap.flags = CEPH_CAP_FLAG_AUTH;
  st.rstat.rbytes = 2048;
  ASSERT_EQ(in.get(), client->add_update_inode(&st, {}, &auth, myperm, 19));
  EXPECT_EQ(2048, in->rstat.rbytes);
  EXPECT_EQ(19u, in->rstat_seq);

  // Readdir replies issue caps: the session holding the auth cap provides
  // rstat, one holding a replica cap does not, whatever the inode version.
  st.cap.caps = CEPH_CAP_PIN;
  st.cap.cap_id = 1;
  st.cap.seq = 1;
  st.cap.realm = CEPH_INO_ROOT;
  st.rstat.rbytes = 16384;
  ASSERT_EQ(in.get(), client->add_update_inode(&st, {}, &auth, myperm, 23));
  ASSERT_NE(nullptr, in->auth_cap);
  EXPECT_EQ(&auth, in->auth_cap->session);
  EXPECT_EQ(16384, in->rstat.rbytes);
  EXPECT_EQ(23u, in->rstat_seq);

  st.cap.flags = 0;
  st.cap.cap_id = 2;
  st.version = 10;
  st.rstat.rbytes = 512;
  ASSERT_EQ(in.get(), client->add_update_inode(&st, {}, &replica, myperm, 24));
  EXPECT_EQ(2u, in->caps.size());
  EXPECT_EQ(&auth, in->auth_cap->session);
  EXPECT_EQ(16384, in->rstat.rbytes);
  EXPECT_EQ(23u, in->rstat_seq);
}

/*
 * The readdir cache tests below hand the client fabricated readdir replies.
 * A reply holds a DirStat, the number of dentries, flags, and one (name,
 * LeaseStat, InodeStat) per dentry; the encoders here mirror
 * CDir::encode_dirstat() and CInode::encode_inodestat() for a session that
 * understands CEPHFS_FEATURE_REPLY_ENCODING.
 */
namespace {

void encode_fake_dirstat(bufferlist& bl, frag_t fg)
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(fg, bl);
  encode((__s32)0, bl);				// auth
  encode(std::set<__s32>{0}, bl);		// dist
  ENCODE_FINISH(bl);
}

void encode_fake_lease(bufferlist& bl)
{
  using ceph::encode;
  ENCODE_START(2, 1, bl);
  encode((__u16)0, bl);				// mask: no lease to honour
  encode((__u32)0, bl);				// duration_ms
  encode((__u32)0, bl);				// seq
  encode(std::string(), bl);			// alternate_name
  ENCODE_FINISH(bl);
}

void encode_fake_inodestat(bufferlist& bl, inodeno_t ino, uint32_t mode,
                          const ceph_mds_reply_cap& cap = {})
{
  using ceph::encode;
  ENCODE_START(6, 1, bl);
  encode(ino, bl);
  encode(snapid_t(CEPH_NOSNAP), bl);
  encode((__u32)0, bl);				// rdev
  encode((version_t)1, bl);			// version
  encode((version_t)1, bl);			// xattr_version
  encode(cap, bl);
  ceph_file_layout legacy_layout;
  memset(&legacy_layout, 0, sizeof(legacy_layout));
  encode(legacy_layout, bl);
  encode(utime_t(), bl);			// ctime
  encode(utime_t(), bl);			// mtime
  encode(utime_t(), bl);			// atime
  encode((__u32)0, bl);				// time_warp_seq
  encode((uint64_t)0, bl);			// size
  encode((uint64_t)0, bl);			// max_size
  encode((uint64_t)0, bl);			// truncate_size
  encode((__u32)1, bl);				// truncate_seq
  encode(mode, bl);
  encode((__u32)0, bl);				// uid
  encode((__u32)0, bl);				// gid
  encode((__u32)1, bl);				// nlink
  encode((int64_t)0, bl);			// dirstat.nfiles
  encode((int64_t)0, bl);			// dirstat.nsubdirs
  encode((int64_t)0, bl);			// rstat.rbytes
  encode((int64_t)0, bl);			// rstat.rfiles
  encode((int64_t)0, bl);			// rstat.rsubdirs
  encode(utime_t(), bl);			// rstat.rctime
  encode(fragtree_t(), bl);
  encode(std::string(), bl);			// symlink
  ceph_dir_layout dir_layout;
  memset(&dir_layout, 0, sizeof(dir_layout));
  encode(dir_layout, bl);
  encode(bufferlist(), bl);			// xattrbl
  encode((version_t)CEPH_INLINE_NONE, bl);	// inline_version
  encode(bufferlist(), bl);			// inline_data
  encode(quota_info_t(), bl);
  encode(std::string(), bl);			// layout.pool_ns
  encode(utime_t(), bl);			// btime
  encode((uint64_t)0, bl);			// change_attr
  encode((mds_rank_t)-1, bl);			// dir_pin
  encode(utime_t(), bl);			// snap_btime
  encode((int64_t)0, bl);			// rstat.rsnaps
  encode(std::map<std::string,std::string>(), bl);	// snap_metadata
  encode(false, bl);				// fscrypt
  ENCODE_FINISH(bl);
}

uint64_t fake_ino_base()
{
  // apart from the mounted filesystem, so these replies cannot disturb it
  return (1ULL << 63) | ((uint64_t)getpid() << 32);
}

// the inode of a fabricated child, so a name keeps it across replies
inodeno_t fake_child_ino(const std::string& name)
{
  static std::map<std::string, uint64_t> inos;
  auto [it, inserted] = inos.emplace(name, 0);
  if (inserted)
    it->second = fake_ino_base() + 1 + inos.size();
  return inodeno_t(it->second);
}

InodeRef make_fake_dir(ClientScaffold *client, MetaSession *session,
		       const UserPerm& perms)
{
  InodeStat st;
  st.vino = vinodeno_t(fake_ino_base(), CEPH_NOSNAP);
  st.version = 2;
  st.mode = S_IFDIR | 0755;
  st.nlink = 1;
  st.dirstat.nfiles = 1;   // an empty dirstat would mark the dir complete
  st.inline_version = CEPH_INLINE_NONE;
  st.dir_pin = -1;
  memset(&st.cap, 0, sizeof(st.cap));
  memset(&st.dir_layout, 0, sizeof(st.dir_layout));
  return InodeRef(client->add_update_inode(&st, {}, session, perms, 0));
}

// Hand the client one readdir reply for 'dirp', as an mds would.
void inject_readdir_reply(ClientScaffold *client, dir_result_t *dirp,
			  MetaSession *session, Inode *diri, frag_t fg,
			  bool hash_order, bool end,
			  const std::vector<std::string>& names,
			  const UserPerm& perms,
			  uint32_t mode = S_IFREG | 0644)
{
  using ceph::encode;
  bufferlist bl;
  encode_fake_dirstat(bl, fg);
  encode((__u32)names.size(), bl);
  __u16 flags = 0;
  if (end)
    flags |= CEPH_READDIR_FRAG_END;
  if (hash_order)
    flags |= CEPH_READDIR_HASH_ORDER;
  encode(flags, bl);
  for (const auto& name : names) {
    encode(name, bl);
    encode_fake_lease(bl);
    encode_fake_inodestat(bl, fake_child_ino(name), mode);
  }

  auto reply = ceph::make_message<MClientReply>();
  reply->set_extra_bl(bl);

  MetaRequest *request = new MetaRequest(CEPH_MDS_OP_READDIR);
  request->head.args.readdir.frag = fg;
  request->set_caller_perms(perms);
  request->dirp = dirp;
  request->reply = reply;
  client->insert_readdir_results(request, session, diri);
  // never registered with the client, so drop it here
  if (request->_put())
    delete request;
}

// what readdir_cache holds, as (name, offset) pairs
std::vector<std::pair<std::string, int64_t>> cached_listing(Inode *diri)
{
  std::vector<std::pair<std::string, int64_t>> entries;
  if (diri->dir) {
    for (Dentry *dn : diri->dir->readdir_cache)
      entries.emplace_back(dn->name, dn->offset);
  }
  return entries;
}

// free the fabricated dentries, as trim_cache() would while unmounting
void drop_fake_dir(ClientScaffold *client, const InodeRef& diri)
{
  while (diri->dir && !diri->dir->dentries.empty())
    client->unlink(diri->dir->dentries.begin()->second, false, false);
}

// so the client decodes the replies below with the newest encoding, rather
// than asking a connection these fabricated replies do not have
void enable_reply_encoding(MetaSession *session)
{
  // std::vector, or the bits would be taken for a value
  session->mds_features =
    feature_bitset_t(std::vector<size_t>{CEPHFS_FEATURE_REPLY_ENCODING});
  ceph_assert(session->mds_features.test(CEPHFS_FEATURE_REPLY_ENCODING));
}

} // anonymous namespace
TEST_F(TestClient, ReaddirCacheDropsFreedDentry) {
  std::scoped_lock lock(client->client_lock);
  MetaSession session(0, {}, {});
  enable_reply_encoding(&session);
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;   // one frag: both the leftmost and the rightmost

  // A pass starts at the beginning of the directory and holds 'a' and 'b'.
  dir_result_t listing(diri.get(), myperm);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       false, {"a", "b"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  EXPECT_TRUE(diri->dir->readdir_pass.active);
  ASSERT_EQ(2u, diri->dir->readdir_cache.size());

  // Trimming 'a' takes the two steps _try_to_trim_inode() does: the inode of
  // a dentry in another directory's cache goes first, the dentry itself only
  // once it is null.  Neither bumps dir_release_count or dir_ordered_count,
  // so nothing but this would tell the pass that its cache lost an entry.
  Dentry *dn = diri->dir->dentries.at("a");
  client->unlink(dn, true, true);   // keep dir, keep dentry
  client->trim_dentry(dn);          // frees dn
  EXPECT_EQ(1u, diri->dir->dentries.size());

  // readdir_cache holds no reference, so it may not keep the freed dentry:
  // the pass would compare the offset of an entry that is gone.
  ASSERT_TRUE(diri->dir->readdir_cache.empty());

  // and the rest of the directory cannot complete that cache either
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"c"}, myperm);
  EXPECT_FALSE(diri->flags & I_DIR_ORDERED);
  EXPECT_TRUE(diri->dir->readdir_cache.empty());
}

TEST_F(TestClient, ReaddirCacheDropsReplyNumberedOutsidePass) {
  std::scoped_lock lock(client->client_lock);
  MetaSession session(0, {}, {});
  enable_reply_encoding(&session);
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), myperm);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       false, {"a", "b"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_EQ(2u, diri->dir->readdir_cache.size());
  const int64_t b_offset = diri->dir->dentries.at("b")->offset;

  // A second stream resumes after 'a' with an ordinal counted in a dentry
  // order this pass does not follow, e.g. one from an earlier generation of
  // the directory.  Its reply cannot extend the pass, but it still numbers
  // the dentries it lists: 'b' is given the ordinal 'a' holds in the cache.
  dir_result_t stale(diri.get(), myperm);
  stale.last_name = "a";
  stale.next_offset = 2;
  stale.next_offset_pass = 0;
  inject_readdir_reply(client, &stale, &session, diri.get(), fg, false,
		       false, {"b"}, myperm);
  EXPECT_NE(b_offset, diri->dir->dentries.at("b")->offset);

  // With two dentries on one ordinal the cache no longer lists the
  // directory: a stream resuming at the ordinal after 'a' would skip 'b'.
  ASSERT_TRUE(diri->dir->readdir_cache.empty());

  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"c"}, myperm);
  EXPECT_FALSE(diri->flags & I_DIR_ORDERED);
}

