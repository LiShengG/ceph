// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <algorithm>
#include <iostream>
#include <limits>
#include <errno.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include "TestClient.h"
#include "client/Dentry.h"
#include "client/Dir.h"
#include "client/Inode.h"
#include "messages/MClientQuota.h"
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
                          const ceph_mds_reply_cap& cap = {},
                          snapid_t snap = CEPH_NOSNAP)
{
  using ceph::encode;
  ENCODE_START(9, 1, bl);
  encode(ino, bl);
  encode(snap, bl);
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
  encode(false, bl);                            // legacy fscrypt flag
  encode(std::vector<uint8_t>{}, bl);            // fscrypt_auth
  encode(std::vector<uint8_t>{}, bl);            // fscrypt_file
  encode(InodeStat{}.optmetadata, bl);          // main optional metadata
  encode(inodeno_t{}, bl);                      // subvolume_id
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
		       const UserPerm& perms, snapid_t snap = CEPH_NOSNAP)
{
  InodeStat st;
  st.vino = vinodeno_t(fake_ino_base(), snap);
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
			  uint32_t mode = S_IFREG | 0644,
                          Inode *other = nullptr, snapid_t entry_snap = CEPH_NOSNAP)
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
    encode_fake_inodestat(bl, fake_child_ino(name), mode, {}, entry_snap);
  }

  auto reply = ceph::make_message<MClientReply>();
  reply->set_extra_bl(bl);

  MetaRequest *request = new MetaRequest(other ? CEPH_MDS_OP_READDIR_SNAPDIFF : CEPH_MDS_OP_READDIR);
  if (other)
    request->head.args.snapdiff.frag = fg;
  else
    request->head.args.readdir.frag = fg;
  request->set_caller_perms(perms);
  request->dirp = dirp;
  request->reply = reply;
  client->insert_readdir_results(request, session, diri, other);
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

struct ReaddirTestSession : MetaSession {
  ReaddirTestSession() : MetaSession(0, {}, {}) {
    enable_reply_encoding(this);
  }
};

} // anonymous namespace

TEST_F(TestClient, ReaddirCacheDropsFreedDentry) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;   // one frag: both the leftmost and the rightmost

  // A pass starts at the beginning of the directory and holds 'a' and 'b'.
  dir_result_t listing(diri.get(), myperm, -1);
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
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), myperm, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       false, {"a", "b"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_EQ(2u, diri->dir->readdir_cache.size());
  const int64_t b_offset = diri->dir->dentries.at("b")->offset;

  // A second stream resumes after 'a' with an ordinal counted in a dentry
  // order this pass does not follow, e.g. one from an earlier generation of
  // the directory.  Its reply cannot extend the pass, but it still numbers
  // the dentries it lists: 'b' is given the ordinal 'a' holds in the cache.
  dir_result_t stale(diri.get(), myperm, -1);
  stale.last_name = "a";
  stale.continuation.offset = 2;
  stale.continuation.pass = 0;
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

namespace {

// A stream resumes after 'a' from the cache the pass completed, as the cache
// path does when it goes back to the mds, and gets 'reply' for the rest of
// the directory.
void check_resumed_reply(ClientScaffold *client, const UserPerm& perms,
			 const std::vector<std::string>& reply, bool kept)
{
  SCOPED_TRACE(::testing::Message() << "reply " << ::testing::PrintToString(reply));
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, perms);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), perms, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"a", "b", "c"}, perms);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());
  const auto complete = cached_listing(diri.get());
  const uint64_t pass_id = diri->dir->readdir_pass.id;

  dir_result_t resumed(diri.get(), perms, -1);
  resumed.offset = complete[0].second + 1;
  resumed.last_name = "a";
  resumed.continuation.offset = dir_result_t::fpos_low(resumed.offset);
  resumed.continuation.pass = pass_id;
  inject_readdir_reply(client, &resumed, &session, diri.get(), fg, false,
		       true, reply, perms);
  if (kept) {
    // the reply numbers its dentries as the cache does: keep the cache
    EXPECT_TRUE(diri->is_complete_and_ordered());
    EXPECT_EQ(complete, cached_listing(diri.get()));
    EXPECT_EQ(pass_id, resumed.continuation.pass);
  } else {
    // the cache lists another directory than the mds does, so what the
    // client holds of it is stale: neither is it complete any more
    EXPECT_FALSE(diri->flags & I_DIR_ORDERED);
    EXPECT_FALSE(diri->flags & I_COMPLETE);
    EXPECT_TRUE(diri->dir->readdir_cache.empty());
  }
}

} // anonymous namespace

TEST_F(TestClient, ReaddirCacheCheckedByReplyInItsOrder) {
  check_resumed_reply(client, myperm, {"b", "c"}, true);
  // a name the cache lists is missing from the reply
  check_resumed_reply(client, myperm, {"c"}, false);
  // the cache goes on past the end of the reply
  check_resumed_reply(client, myperm, {"b"}, false);
  // the reply goes on past the end of the cache
  check_resumed_reply(client, myperm, {"b", "c", "d"}, false);
}

TEST_F(TestClient, ReaddirCacheRebuiltByRelisting) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  // One reply lists the whole directory: the pass completes and publishes
  // its cache as the ordered listing.
  dir_result_t listing(diri.get(), myperm, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"a", "b", "c"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());
  ASSERT_EQ(3u, diri->dir->readdir_cache.size());

  // The directory is listed again from its start, e.g. after the cache path
  // gave up on a stale rstat, and the mds no longer reports 'b'.  The cache
  // has to follow the reply: keeping it would go on listing a name the mds
  // does not have, on ordinals the reply has just renumbered.  Nor may the
  // directory be complete while the client still holds 'b': readdir would
  // leave out a name lookup finds.
  dir_result_t again(diri.get(), myperm, -1);
  inject_readdir_reply(client, &again, &session, diri.get(), fg, false,
		       true, {"a", "c"}, myperm);
  EXPECT_FALSE(diri->flags & I_COMPLETE);
  EXPECT_TRUE(diri->dir->readdir_cache.empty());

  // Once 'b' is gone from the client too, the next listing completes the
  // directory, on the ordinals it gives.
  client->unlink(diri->dir->dentries.at("b"), true, false);
  dir_result_t third(diri.get(), myperm, -1);
  inject_readdir_reply(client, &third, &session, diri.get(), fg, false,
		       true, {"a", "c"}, myperm);
  EXPECT_TRUE(diri->is_complete_and_ordered());
  const std::vector<std::pair<std::string, int64_t>> expected = {
    {"a", dir_result_t::make_fpos(fg, 2, false)},
    {"c", dir_result_t::make_fpos(fg, 3, false)},
  };
  EXPECT_EQ(expected, cached_listing(diri.get()));
}

TEST_F(TestClient, ReaddirCacheCheckedByListingFromStart) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), myperm, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"a", "b", "c"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());
  const auto complete = cached_listing(diri.get());
  const uint64_t pass_id = diri->dir->readdir_pass.id;

  // Nothing changed since: a new listing, page by page, numbers the
  // dentries as the cache does, and leaves it to streams reading from it.
  dir_result_t again(diri.get(), myperm, -1);
  inject_readdir_reply(client, &again, &session, diri.get(), fg, false,
		       false, {"a", "b"}, myperm);
  EXPECT_TRUE(diri->is_complete_and_ordered());
  EXPECT_EQ(complete, cached_listing(diri.get()));
  EXPECT_EQ(pass_id, again.continuation.pass);
  inject_readdir_reply(client, &again, &session, diri.get(), fg, false,
		       true, {"c"}, myperm);
  EXPECT_TRUE(diri->is_complete_and_ordered());
  EXPECT_EQ(complete, cached_listing(diri.get()));
  EXPECT_EQ(pass_id, diri->dir->readdir_pass.id);
}

TEST_F(TestClient, ReaddirCacheCheckedAfterRemoval) {
  for (const std::string removed : {"c", "a"}) {
    SCOPED_TRACE("removed " + removed);
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    const frag_t fg;

    std::vector<std::string> names = {"a", "b", "c"};
    dir_result_t listing(diri.get(), myperm, -1);
    inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
			 true, names, myperm);
    ASSERT_NE(nullptr, diri->dir);
    ASSERT_TRUE(diri->is_complete_and_ordered());

    // its inode was trimmed, and the name is gone: a null dentry, still in
    // the cache, which skips it
    client->unlink(diri->dir->dentries.at(removed), true, true);
    ASSERT_TRUE(diri->is_complete_and_ordered());
    names.erase(std::find(names.begin(), names.end(), removed));

    dir_result_t again(diri.get(), myperm, -1);
    inject_readdir_reply(client, &again, &session, diri.get(), fg, false,
			 true, names, myperm);
    // the directory is still what the client holds
    EXPECT_TRUE(diri->flags & I_COMPLETE);
    if (removed == "c") {
      // nothing after the null dentry to renumber
      EXPECT_TRUE(diri->is_complete_and_ordered());
    } else {
      // the ordinals after it in the frag moved
      EXPECT_FALSE(diri->flags & I_DIR_ORDERED);
    }
  }
}

TEST_F(TestClient, ReaddirCacheTrimsNullDentryKeepingComplete) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), myperm, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"a", "b"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());

  // 'a' is a null dentry, e.g. its inode was trimmed, and the lru trims
  // it: the cache, which holds it, has to go, but the directory is still
  // complete
  Dentry *dn = diri->dir->dentries.at("a");
  client->unlink(dn, true, true);
  client->trim_dentry(dn);
  EXPECT_TRUE(diri->dir->readdir_cache.empty());
  EXPECT_FALSE(diri->flags & I_DIR_ORDERED);
  EXPECT_TRUE(diri->flags & I_COMPLETE);
}

TEST_F(TestClient, ReaddirCacheKeepsUncachedDentryTrim) {
  for (bool complete : {false, true}) {
    SCOPED_TRACE(complete);
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    client->clear_dir_complete_and_ordered(diri.get(), true);
    dir_result_t listing(diri.get(), myperm, -1);
    inject_readdir_reply(client, &listing, &session, diri.get(), frag_t(), false,
                         complete, {"a", "b"}, myperm);
    const auto cached = cached_listing(diri.get());
    ASSERT_EQ(2u, cached.size());
    const auto ordered_count = diri->dir_ordered_count;

    // Negative lookups need not belong to the listing. Even sharing an
    // offset with a cached dentry does not make the negative dentry a member.
    for (int64_t offset : {int64_t(0), cached.front().second}) {
      Dentry *dn = client->link(diri->dir, "missing", nullptr, nullptr);
      dn->offset = offset;
      client->trim_dentry(dn);
      EXPECT_EQ(cached, cached_listing(diri.get()));
      EXPECT_EQ(ordered_count, diri->dir_ordered_count);
      EXPECT_EQ(complete, diri->is_complete_and_ordered());
    }
    if (!complete) {
      inject_readdir_reply(client, &listing, &session, diri.get(), frag_t(), false,
                           true, {"c"}, myperm);
      EXPECT_TRUE(diri->is_complete_and_ordered());
      EXPECT_EQ(3u, diri->dir->readdir_cache.size());
    }
  }
}

TEST_F(TestClient, ReaddirCacheKeepsOldCursorAcrossHash) {
  // These names have distinct, increasing hashes with the default Linux hash.
  const std::vector<std::string> names = {"alpha", "bravo", "charlie"};
  for (unsigned prefix : {1u, 2u, 3u}) {
    SCOPED_TRACE(prefix);
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    client->clear_dir_complete_and_ordered(diri.get(), true);
    ASSERT_LT(ceph_frag_value(diri->hash_dentry_name(names[0])),
              ceph_frag_value(diri->hash_dentry_name(names[1])));
    ASSERT_LT(ceph_frag_value(diri->hash_dentry_name(names[1])),
              ceph_frag_value(diri->hash_dentry_name(names[2])));
    dir_result_t old(diri.get(), myperm, -1);
    inject_readdir_reply(client, &old, &session, diri.get(), frag_t(), true,
                         false, {names[0]}, myperm);
    client->clear_dir_complete_and_ordered(diri.get(), false);
    dir_result_t fresh(diri.get(), myperm, -1);
    inject_readdir_reply(client, &fresh, &session, diri.get(), frag_t(), true,
                         prefix == names.size(),
                         {names.begin(), names.begin() + prefix}, myperm);
    const auto cached = cached_listing(diri.get());
    const auto pass_id = diri->dir->readdir_pass.id;
    ASSERT_NE(pass_id, old.continuation.pass);

    // Depending on how far the new pass got, this reply is beyond it,
    // overlaps it, or verifies its completed cache. None needs the old
    // ordinal once the reply has moved to the next hash.
    inject_readdir_reply(client, &old, &session, diri.get(), frag_t(), true,
                         true, {names[1], names[2]}, myperm);
    EXPECT_EQ(pass_id, old.buffer_offset_pass);
    if (prefix == 1) {
      EXPECT_EQ(cached, cached_listing(diri.get()));
      EXPECT_FALSE(diri->flags & I_COMPLETE);
      inject_readdir_reply(client, &fresh, &session, diri.get(), frag_t(), true,
                           true, {names[1], names[2]}, myperm);
    }
    ASSERT_TRUE(diri->is_complete_and_ordered());
    ASSERT_EQ(names.size(), diri->dir->readdir_cache.size());
    for (size_t i = 0; i < names.size(); ++i)
      EXPECT_EQ(names[i], diri->dir->readdir_cache[i]->name);
  }
}

TEST_F(TestClient, ReaddirCacheChecksHashGapAfterOldCursor) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  dir_result_t old(diri.get(), myperm, -1);
  inject_readdir_reply(client, &old, &session, diri.get(), frag_t(), true,
                       false, {"alpha"}, myperm);
  client->clear_dir_complete_and_ordered(diri.get(), false);
  dir_result_t fresh(diri.get(), myperm, -1);
  inject_readdir_reply(client, &fresh, &session, diri.get(), frag_t(), true,
                       true, {"alpha", "bravo", "charlie"}, myperm);
  ASSERT_TRUE(diri->is_complete_and_ordered());

  // Starting verification at the first returned hash would overlook the
  // cached name whose hash lies between the cursor and that first entry.
  inject_readdir_reply(client, &old, &session, diri.get(), frag_t(), true,
                       true, {"charlie"}, myperm);
  EXPECT_FALSE(diri->flags & I_COMPLETE);
  EXPECT_TRUE(diri->dir->readdir_cache.empty());
}

TEST_F(TestClient, ReaddirPassKeepsOldHashCollisionProtection) {
  for (bool crosses_hash : {false, true}) {
    SCOPED_TRACE(crosses_hash);
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    client->clear_dir_complete_and_ordered(diri.get(), true);
    const unsigned hash = ceph_frag_value(diri->hash_dentry_name("aoao"));
    ASSERT_EQ(hash, ceph_frag_value(diri->hash_dentry_name("aobd")));
    ASSERT_EQ(hash, ceph_frag_value(diri->hash_dentry_name("bdao")));
    ASSERT_LT(hash, ceph_frag_value(diri->hash_dentry_name("alpha")));
    dir_result_t old(diri.get(), myperm, -1);
    inject_readdir_reply(client, &old, &session, diri.get(), frag_t(), true,
                         false, {"aobd"}, myperm);
    client->clear_dir_complete_and_ordered(diri.get(), false);
    dir_result_t fresh(diri.get(), myperm, -1);
    inject_readdir_reply(client, &fresh, &session, diri.get(), frag_t(), true,
                         false, {"aoao"}, myperm);
    const auto cached = cached_listing(diri.get());
    inject_readdir_reply(client, &old, &session, diri.get(), frag_t(), true,
                         true, {crosses_hash ? "alpha" : "bdao"}, myperm);
    EXPECT_FALSE(diri->flags & I_COMPLETE);
    if (crosses_hash) {
      // The reply numbers alpha correctly, but the pass has not yet seen
      // aobd: crossing a hash must not bridge that gap in the old hash.
      EXPECT_EQ(cached, cached_listing(diri.get()));
      inject_readdir_reply(client, &fresh, &session, diri.get(), frag_t(), true,
                           true, {"aobd", "alpha"}, myperm);
      EXPECT_TRUE(diri->is_complete_and_ordered());
    } else {
      // bdao would reuse an ordinal counted before aoao was inserted.
      EXPECT_TRUE(diri->dir->readdir_cache.empty());
    }
  }
}

TEST_F(TestClient, ReaddirPassContinuesAfterFragmentSplit) {
  for (bool empty : {false, true}) {
    SCOPED_TRACE(empty);
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    client->clear_dir_complete_and_ordered(diri.get(), true);
    const frag_t left(0, 1);
    const frag_t right = left.next();
    diri->dirfragtree.clear();
    diri->dirfragtree.split(frag_t(), 1);
    dir_result_t listing(diri.get(), myperm, -1);
    inject_readdir_reply(client, &listing, &session, diri.get(), left, false,
                         true, {"a"}, myperm);

    // The next reply's inode trace carries a split that happened after
    // the pass saved the boundary following left.
    diri->dirfragtree.split(right, 1);
    listing.last_name.clear();
    inject_readdir_reply(client, &listing, &session, diri.get(), right.left_child(),
                         false, true, empty ? std::vector<std::string>{} :
                         std::vector<std::string>{"b"}, myperm);
    listing.last_name.clear();
    inject_readdir_reply(client, &listing, &session, diri.get(), right.right_child(),
                         false, true, {"c"}, myperm);
    EXPECT_TRUE(diri->is_complete_and_ordered());
    EXPECT_EQ(empty ? 2u : 3u, diri->dir->readdir_cache.size());
  }
}

TEST_F(TestClient, ReaddirPassDoesNotSkipFragmentStart) {
  for (bool within_frag : {false, true}) {
    SCOPED_TRACE(within_frag);
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    client->clear_dir_complete_and_ordered(diri.get(), true);
    const frag_t left(0, 1);
    const frag_t right = left.next();
    diri->dirfragtree.clear();
    diri->dirfragtree.split(frag_t(), 1);
    dir_result_t listing(diri.get(), myperm, -1);
    inject_readdir_reply(client, &listing, &session, diri.get(), left, false,
                         true, {"a"}, myperm);
    const auto cached = cached_listing(diri.get());
    dir_result_t ahead(diri.get(), myperm, -1);
    frag_t fg = right;
    if (within_frag) {
      ahead.last_name = "b";
      ahead.continuation.offset = 3;
      ahead.continuation.pass = diri->dir->readdir_pass.id;
    } else {
      diri->dirfragtree.split(right, 1);
      fg = right.right_child();
    }
    // Matching the boundary's hash is not enough when the reply skips the
    // start of the frag. Nor may it skip the first child of a split frag.
    inject_readdir_reply(client, &ahead, &session, diri.get(), fg, false,
                         true, {"c"}, myperm);
    EXPECT_FALSE(diri->flags & I_COMPLETE);
    EXPECT_EQ(cached, cached_listing(diri.get()));
  }
}

TEST_F(TestClient, ReaddirPassGoesOnWithOverlappingListing) {
  for (bool agree : {true, false}) {
    SCOPED_TRACE(agree ? "listings agree" : "second listing misses 'a'");
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    const frag_t fg;

    dir_result_t first(diri.get(), myperm, -1);
    inject_readdir_reply(client, &first, &session, diri.get(), fg, false,
			 false, {"a", "b"}, myperm);
    ASSERT_NE(nullptr, diri->dir);
    ASSERT_TRUE(diri->dir->readdir_pass.active);

    // A second stream lists the directory from its start as well: what
    // the pass has seen has to come again, one after the other, at the
    // offsets the pass gave, before the reply takes the pass further.
    dir_result_t second(diri.get(), myperm, -1);
    std::vector<std::string> names = {"a", "b", "c"};
    if (!agree)
      names.erase(names.begin());
    inject_readdir_reply(client, &second, &session, diri.get(), fg, false,
			 true, names, myperm);
    if (agree) {
      EXPECT_TRUE(diri->is_complete_and_ordered());
      EXPECT_EQ((std::vector<std::pair<std::string, int64_t>>{
		  {"a", dir_result_t::make_fpos(fg, 2, false)},
		  {"b", dir_result_t::make_fpos(fg, 3, false)},
		  {"c", dir_result_t::make_fpos(fg, 4, false)}}),
		cached_listing(diri.get()));
    } else {
      // the client holds 'a', which the mds no longer lists
      EXPECT_FALSE(diri->flags & I_COMPLETE);
      EXPECT_TRUE(diri->dir->readdir_cache.empty());
      EXPECT_FALSE(diri->dir->readdir_pass.active);
    }
  }
}

TEST_F(TestClient, ReaddirCacheKeptByReplyPastPass) {
  for (bool moved : {false, true}) {
    SCOPED_TRACE(moved ? "reply moves a cached dentry" : "reply past the pass");
    std::scoped_lock lock(client->client_lock);
    ReaddirTestSession session;
    InodeRef diri = make_fake_dir(client, &session, myperm);
    auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
    const frag_t fg;

    dir_result_t listing(diri.get(), myperm, -1);
    inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
			 false, {"a", "b"}, myperm);
    ASSERT_NE(nullptr, diri->dir);
    ASSERT_TRUE(diri->dir->readdir_pass.active);
    const auto seen = cached_listing(diri.get());
    ASSERT_EQ(2u, seen.size());

    // Another stream went on after 'c', in the order of this pass, and gets
    // the next page: it starts past what the pass has seen so far.
    dir_result_t ahead(diri.get(), myperm, -1);
    ahead.last_name = "c";
    ahead.continuation.offset = 5;
    ahead.continuation.pass = diri->dir->readdir_pass.id;
    inject_readdir_reply(client, &ahead, &session, diri.get(), fg, false,
			 false, {moved ? "b" : "d"}, myperm);
    if (moved) {
      // 'b' cannot be both where the pass saw it and after 'c'
      EXPECT_TRUE(diri->dir->readdir_cache.empty());
      continue;
    }
    EXPECT_EQ(seen, cached_listing(diri.get()));

    // and the pass completes the cache as if the page had not been read
    inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
			 true, {"c", "d"}, myperm);
    EXPECT_TRUE(diri->is_complete_and_ordered());
    EXPECT_EQ(4u, diri->dir->readdir_cache.size());
  }
}

namespace {

// Hands each entry of a cached listing over, and relists the directory from
// its start while it has client_lock given up for 'rebuild_at'.
struct RelistingReader {
  ClientScaffold *client;
  MetaSession *session;
  Inode *diri;
  UserPerm perms;
  std::string rebuild_at;
  std::vector<std::string> relisted;
  std::vector<std::string> names;

  static int cb(void *p, struct dirent *de, struct ceph_statx *, off_t,
		Inode *) {
    auto& reader = *static_cast<RelistingReader *>(p);
    reader.names.emplace_back(de->d_name);
    if (reader.names.back() == reader.rebuild_at) {
      std::scoped_lock lock(reader.client->client_lock);
      // what the mds no longer lists is gone from the client as well
      std::vector<Dentry *> gone;
      for (auto& [name, dn] : reader.diri->dir->dentries) {
	if (std::find(reader.relisted.begin(), reader.relisted.end(), name) ==
	    reader.relisted.end())
	  gone.push_back(dn);
      }
      for (Dentry *dn : gone)
	reader.client->unlink(dn, true, false);
      dir_result_t again(reader.diri, reader.perms, -1);
      inject_readdir_reply(reader.client, &again, reader.session, reader.diri,
			   frag_t(), false, true, reader.relisted, reader.perms);
    }
    return 0; // go on
  }
};

} // anonymous namespace

TEST_F(TestClient, ReaddirCacheRefindsEntryAfterCallback) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, myperm);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), myperm, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, {"a", "b", "c"}, myperm);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());

  // While 'b' is handed over, 'a' and 'c' are removed and the directory is
  // listed again: readdir_cache shrinks to one entry, and is complete and
  // ordered again by the time the reader takes the lock back.  An iterator
  // kept across the callback would point past the end of the vector.  The
  // offset after 'b' counts in the order of the old pass, 'b' now has the
  // first ordinal: the reader goes on after where the cache has 'b'.
  RelistingReader reader{client, &session, diri.get(), myperm, "b", {"b"}};
  dir_result_t reading(diri.get(), myperm, -1);
  reading.offset = dir_result_t::make_fpos(fg, 2, false); // skip . and ..
  EXPECT_EQ(0, client->_readdir_cache_cb(&reading, RelistingReader::cb,
					 &reader, 0, false));
  EXPECT_EQ((std::vector<std::string>{"a", "b"}), reader.names);
  EXPECT_TRUE(reading.at_end());
  ASSERT_TRUE(diri->is_complete_and_ordered());
  EXPECT_EQ((std::vector<std::pair<std::string, int64_t>>{
	      {"b", dir_result_t::make_fpos(fg, 2, false)}}),
	    cached_listing(diri.get()));
}

namespace {

struct CachedNames {
  std::vector<std::string> names;

  static int cb(void *p, struct dirent *de, struct ceph_statx *, off_t,
		Inode *) {
    static_cast<CachedNames *>(p)->names.emplace_back(de->d_name);
    return 0; // go on
  }
};

// A stream has returned the cached listing up to 'last' when a pass rebuilds
// readdir_cache for the directory now holding 'relisted', and goes on from
// the cache.  'moved' tells whether the cursor stands right after last_name,
// as it does when it came from the cache path; otherwise it stands inside an
// mds reply that last_name ends.
void check_cursor_of_old_pass(ClientScaffold *client, const UserPerm& perms,
			      const std::vector<std::string>& listed,
			      const std::string& last,
			      const std::vector<std::string>& relisted,
			      bool moved,
			      const std::vector<std::string>& expected)
{
  SCOPED_TRACE(::testing::Message() << "relisted "
	       << ::testing::PrintToString(relisted) << " moved " << moved);
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  InodeRef diri = make_fake_dir(client, &session, perms);
  auto cleanup = make_scope_guard([&] { drop_fake_dir(client, diri); });
  const frag_t fg;

  dir_result_t listing(diri.get(), perms, -1);
  inject_readdir_reply(client, &listing, &session, diri.get(), fg, false,
		       true, listed, perms);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());
  const uint64_t old_pass = diri->dir->readdir_pass.id;

  // where the cache path leaves a stream that returned 'last'
  dir_result_t reading(diri.get(), perms, -1);
  reading.offset = diri->dir->dentries.at(last)->offset + 1;
  reading.offset_pass = old_pass;
  reading.last_name = last;
  reading.continuation.offset = dir_result_t::fpos_low(reading.offset);
  reading.continuation.pass = old_pass;
  if (!moved)
    ++reading.continuation.offset;
  // an mds reply numbered in the old order
  reading.buffer.emplace_back(reading.offset, "d", "",
			      diri->dir->dentries.at(listed.back())->inode);

  for (auto it = diri->dir->dentries.begin();
       it != diri->dir->dentries.end(); ) {
    Dentry *dn = (it++)->second;
    if (std::find(relisted.begin(), relisted.end(), dn->name) ==
	relisted.end())
      client->unlink(dn, true, false);
  }
  // as a create does, see insert_dentry_inode()
  client->clear_dir_complete_and_ordered(diri.get(), false);
  dir_result_t again(diri.get(), perms, -1);
  inject_readdir_reply(client, &again, &session, diri.get(), fg, false,
		       true, relisted, perms);
  ASSERT_TRUE(diri->is_complete_and_ordered());
  ASSERT_NE(old_pass, diri->dir->readdir_pass.id);

  CachedNames names;
  int r = client->_readdir_cache_cb(&reading, CachedNames::cb, &names, 0,
				    false);
  if (moved) {
    EXPECT_EQ(0, r);
    EXPECT_TRUE(reading.at_end());
    // nothing may go on from it in the order of the new pass
    EXPECT_TRUE(reading.buffer.empty());
  } else {
    // the mds path goes on from last_name
    EXPECT_EQ(-EAGAIN, r);
    EXPECT_FALSE(reading.at_end());
    EXPECT_EQ(last, reading.last_name);
  }
  EXPECT_EQ(expected, names.names);
}

} // anonymous namespace

TEST_F(TestClient, ReaddirCacheCursorOfOldPass) {
  // 'b' goes: 'd' gets the ordinal after 'c', where the cursor stands
  check_cursor_of_old_pass(client, myperm, {"b", "c", "d"}, "c", {"c", "d"},
			   true, {"d"});
  // 'a' comes before 'b': 'b' gets the ordinal after it, where the cursor
  // stands
  check_cursor_of_old_pass(client, myperm, {"b", "c"}, "b", {"a", "b", "c"},
			   true, {"c"});
  // nowhere to tell where the cursor stands in the new order
  check_cursor_of_old_pass(client, myperm, {"b", "c", "d"}, "c", {"c", "d"},
			   false, {});
}

namespace {

// Extend the mounted client's map without replacing any real MDS rank.
class ReaddirTestMDSMap : public MDSMap {
public:
  explicit ReaddirTestMDSMap(const MDSMap& original) : MDSMap(original) {}

  void add_rank(mds_rank_t rank) {
    mds_gid_t gid(mds_info.empty() ? 1 : mds_info.rbegin()->first + 1);
    auto& info = mds_info[gid];
    info.global_id = gid;
    info.rank = rank;
    info.state = MDSMap::STATE_ACTIVE;
    in.insert(rank);
    up[rank] = gid;
    set_max_mds(rank + 1);
  }

  // newer than any map the monitors send: Client::handle_mds_map() ignores
  // them, which would otherwise close the session of the added rank
  void outdate_real_maps() {
    epoch = std::numeric_limits<epoch_t>::max();
  }
};

struct ReaddirReplyState {
  std::mutex mutex;
  std::condition_variable changed;
  ceph::ref_t<MClientRequest> request;
  bool reader_done = false;
  bool dispatcher_done = false;
};

class ReaddirTestConnection : public Connection {
  ReaddirReplyState& state;
  const inodeno_t ino;

public:
  ReaddirTestConnection(CephContext *cct, Messenger *messenger,
                       ReaddirReplyState& state, inodeno_t ino)
    : Connection(cct, messenger), state(state), ino(ino) {
    set_peer_type(CEPH_ENTITY_TYPE_MDS);
  }

  bool is_connected() override { return true; }
  entity_addr_t get_peer_socket_addr() const override { return {}; }
  void send_keepalive() override {}
  void mark_down() override {}
  void mark_disposable() override {}

  void shutdown() override {}

  int send_msg(MessageRef&& message) override {
    // Session renewals and cap releases need no response in this short test.
    if (message->get_type() != CEPH_MSG_CLIENT_REQUEST)
      return 0;
    auto request = ceph::ref_cast<MClientRequest>(message);
    EXPECT_EQ(CEPH_MDS_OP_GETATTR, request->get_op());
    EXPECT_EQ(ino, request->get_filepath().get_ino());
    if (request->get_op() != CEPH_MDS_OP_GETATTR ||
        request->get_filepath().get_ino() != ino)
      return -EINVAL;
    std::lock_guard lock(state.mutex);
    EXPECT_FALSE(state.request);
    state.request = std::move(request);
    state.changed.notify_all();
    return 0;
  }
};

struct ReaddirEntries {
  std::vector<std::string> names;
  std::vector<off_t> cookies;

  static int one(void *p, struct dirent *de, struct ceph_statx *,
                 off_t next, Inode *) {
    auto& entries = *static_cast<ReaddirEntries *>(p);
    entries.names.emplace_back(de->d_name);
    entries.cookies.push_back(next);
    return 1; // stop after one accepted entry, like a small readdir buffer
  }
};

} // anonymous namespace

MetaSession *ClientScaffold::install_readdir_test_session(
    const ConnectionRef& con, MDSMap *saved_map)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  *saved_map = *mdsmap;
  mds_rank_t rank = mdsmap->get_max_mds();
  while (mdsmap->is_in(rank) || mdsmap->is_up(rank) ||
         mds_sessions.count(rank))
    ++rank;
  ReaddirTestMDSMap map(*mdsmap);
  map.add_rank(rank);
  map.outdate_real_maps();
  *mdsmap = map;
  auto [it, inserted] = mds_sessions.emplace(
    rank, std::make_shared<MetaSession>(rank, con, entity_addrvec_t{}));
  ceph_assert(inserted);
  auto& session = *it->second;
  session.state = MetaSession::STATE_OPEN;
  session.mds_state = MDSMap::STATE_ACTIVE;
  session.cap_ttl = ceph_clock_now() + utime_t(60, 0);
  enable_reply_encoding(&session);
  return &session;
}

void ClientScaffold::remove_readdir_test_session(
    mds_rank_t rank, const MDSMap& saved_map)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  auto& session = *mds_sessions.at(rank);
  EXPECT_TRUE(session.requests.empty());
  EXPECT_TRUE(session.unsafe_requests.empty());
  EXPECT_TRUE(session.caps.empty());
  mds_sessions.erase(rank);
  // The last map the client took, as none have been since the test map.
  // The next one the monitors send replaces it.
  *mdsmap = saved_map;
}

MetaRequest *ClientScaffold::find_readdir_test_request(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  for (auto& [tid, request] : mds_requests) {
    if (request->inode() == in)
      return request;
  }
  return nullptr;
}

void ClientScaffold::cancel_readdir_test_requests(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  for (auto& [tid, request] : mds_requests) {
    if (request->inode() != in || request->reply)
      continue;
    request->abort(-ECANCELED);
    request->kick = true;
    if (request->caller_cond)
      request->caller_cond->notify_all();
  }
  // Also cover a request waiting for a map update before it can be sent.
  signal_cond_list(waiting_for_mdsmap);
  for (auto& [rank, cap] : in->caps)
    signal_context_list(cap.session->waiting_for_open);
}

namespace {

void check_readdir_cache_after_getattr(ClientScaffold *client,
                                      Messenger *messenger,
                                      const UserPerm& perms, bool rebuild)
{
  SCOPED_TRACE(rebuild ? "partial rebuild" : "no rebuild (control)");
  std::unique_lock lock(client->client_lock);
  // All synthetic requests must follow the caps to our extra session.
  ASSERT_FALSE(client->cct->_conf->client_use_random_mds);
  ReaddirReplyState state;
  auto con = ceph::make_ref<ReaddirTestConnection>(
    client->cct, messenger, state, fake_child_ino("a"));
  MDSMap saved_map;
  MetaSession *session = client->install_readdir_test_session(con, &saved_map);
  const mds_rank_t rank = session->mds_num;
  auto restore_session = make_scope_guard([&] {
    client->remove_readdir_test_session(rank, saved_map);
  });
  InodeRef diri = make_fake_dir(client, session, perms);
  auto drop_inodes = make_scope_guard([&] {
    // No peer owns these caps. Account for their release immediately;
    // queuing it would leave pinned-cap accounting behind with the session.
    while (!session->caps.empty())
      client->remove_cap(*session->caps.begin(), false);
    drop_fake_dir(client, diri);
  });

  const frag_t left(0, 1);
  const frag_t right = left.next();
  diri->dirfragtree.split(frag_t(), 1);
  dir_result_t seed(diri.get(), perms, -1);
  inject_readdir_reply(client, &seed, session, diri.get(), left, false,
                       true, {"a"}, perms, S_IFDIR | 0755);
  seed.last_name.clear(); // _readdir_next_frag() starts a new frag this way
  inject_readdir_reply(client, &seed, session, diri.get(), right, false,
                       true, {"b"}, perms);
  ASSERT_NE(nullptr, diri->dir);
  ASSERT_TRUE(diri->is_complete_and_ordered());
  const std::vector<std::pair<std::string, int64_t>> complete = {
    {"a", dir_result_t::make_fpos(left, 2, false)},
    {"b", dir_result_t::make_fpos(right, 2, false)},
  };
  ASSERT_EQ(complete, cached_listing(diri.get()));

  dir_result_t reader(diri.get(), perms, -1);
  dir_result_t relisting(diri.get(), perms, -1);
  client->start_readdir_test_listing(&reader);
  reader.offset = complete.front().second; // skip . and .., resume at a
  InodeRef a = diri->dir->dentries.at("a")->inode;
  InodeRef b = diri->dir->dentries.at("b")->inode;
  ASSERT_TRUE(a->is_dir());
  a->rstat_seq = reader.listing_seq;
  const int caps = CEPH_CAP_AUTH_SHARED;
  client->add_update_cap(a.get(), session, 1, CEPH_CAP_PIN, 0, 1, 0,
                         diri->ino, CEPH_CAP_FLAG_AUTH, perms);
  client->add_update_cap(b.get(), session, 2, CEPH_CAP_PIN | caps, 0, 1, 0,
                         diri->ino, CEPH_CAP_FLAG_AUTH, perms);
  ASSERT_GE(a->rstat_seq, reader.listing_seq);
  ASSERT_FALSE(a->caps_issued_mask(caps));
  ASSERT_TRUE(b->caps_issued_mask(caps));

  ReaddirEntries entries;
  int result = 0;
  bool stopping = false; // protected by client_lock, including early cleanup
  std::thread worker;
  std::thread dispatcher;
  auto join_threads = make_scope_guard([&] {
    if (!lock.owns_lock())
      lock.lock();
    stopping = true;
    client->cancel_readdir_test_requests(a.get());
    lock.unlock();
    if (worker.joinable())
      worker.join();
    if (dispatcher.joinable())
      dispatcher.join();
    lock.lock(); // inode and session destruction require the client lock
  });
  worker = std::thread([&] {
    {
      std::scoped_lock client_lock(client->client_lock);
      if (!stopping)
        result = client->_readdir_cache_cb(&reader, ReaddirEntries::one,
                                          &entries, caps, false);
    }
    std::lock_guard state_lock(state.mutex);
    state.reader_done = true;
    state.changed.notify_all();
  });
  lock.unlock();
  ceph::ref_t<MClientRequest> getattr;
  {
    std::unique_lock state_lock(state.mutex);
    ASSERT_TRUE(state.changed.wait_for(state_lock, std::chrono::seconds(10),
      [&] { return state.request || state.reader_done; }))
      << "cache reader did not send GETATTR within 10 seconds";
    getattr = state.request;
    ASSERT_TRUE(getattr) << "cache reader returned before requesting attributes";
  }

  // send_message2() ran under client_lock. Acquiring it here proves that A
  // has reached make_request()'s wait and released the lock, not just that
  // the mock connection saw the outgoing message.
  lock.lock();
  MetaRequest *pending = client->find_readdir_test_request(a.get());
  ASSERT_NE(nullptr, pending);
  ASSERT_EQ(getattr->get_tid(), pending->tid);
  ASSERT_EQ(CEPH_MDS_OP_GETATTR, pending->get_op());
  ASSERT_EQ(rank, pending->mds);
  ASSERT_NE(nullptr, pending->caller_cond);
  ASSERT_FALSE(pending->reply);
  ASSERT_EQ(caps, (int)getattr->head.args.getattr.mask);

  if (rebuild) {
    // e.g. a dentry was created: the next listing rebuilds the cache
    client->clear_dir_complete_and_ordered(diri.get(), false);
    client->start_readdir_test_listing(&relisting);
    inject_readdir_reply(client, &relisting, session, diri.get(), left, false,
                         true, {"a"}, perms, S_IFDIR | 0755);
    ASSERT_FALSE(diri->is_complete_and_ordered());
    ASSERT_TRUE(diri->dir->readdir_pass.active);
    ASSERT_EQ((std::vector<std::pair<std::string, int64_t>>{complete.front()}),
              cached_listing(diri.get()));
    ASSERT_EQ(1u, diri->dir->dentries.count("b"));
    ASSERT_EQ(b, diri->dir->dentries.at("b")->inode);
  }

  // A real safe reply updates a's attributes and wakes make_request(); its
  // dispatcher waits for the caller's kickback, just as the messenger does.
  auto reply = ceph::make_message<MClientReply>(*getattr, 0);
  reply->set_src(entity_name_t::MDS(rank));
  reply->set_connection(con);
  reply->head.is_target = 1;
  ceph_mds_reply_cap cap = {};
  cap.caps = CEPH_CAP_PIN | caps;
  cap.cap_id = 1;
  cap.seq = 2;
  cap.realm = diri->ino;
  cap.flags = CEPH_CAP_FLAG_AUTH;
  bufferlist trace;
  encode_fake_inodestat(trace, a->ino, a->mode, cap);
  reply->set_trace(trace);
  dispatcher = std::thread([&] {
    client->handle_client_reply(reply);
    std::lock_guard state_lock(state.mutex);
    state.dispatcher_done = true;
    state.changed.notify_all();
  });
  lock.unlock();
  {
    std::unique_lock state_lock(state.mutex);
    ASSERT_TRUE(state.changed.wait_for(state_lock, std::chrono::seconds(10),
      [&] { return state.reader_done && state.dispatcher_done; }))
      << "GETATTR reply/reader handshake did not finish within 10 seconds";
  }
  worker.join();
  dispatcher.join();
  lock.lock();
  ASSERT_EQ(nullptr, client->find_readdir_test_request(a.get()));
  ASSERT_TRUE(a->caps_issued_mask(caps));

  {
    SCOPED_TRACE(::testing::Message()
      << "cache_size=" << diri->dir->readdir_cache.size()
      << " complete_and_ordered=" << diri->is_complete_and_ordered()
      << " result=" << result
      << " cookies=" << ::testing::PrintToString(entries.cookies)
      << " A.offset=" << reader.offset << " A.at_end=" << reader.at_end());
    // Being last in a partially rebuilt vector does not make a the last
    // entry of the directory. Rejecting the cache and retrying is also OK.
    EXPECT_FALSE(reader.at_end());
    for (off_t cookie : entries.cookies)
      EXPECT_EQ(0, cookie & dir_result_t::END);
    if (result == -EAGAIN) {
      EXPECT_TRUE(rebuild);
      EXPECT_TRUE(entries.names.empty());
      EXPECT_TRUE(entries.cookies.empty());
      EXPECT_EQ(complete.front().second, reader.offset);
    } else {
      EXPECT_EQ(1, result);
      EXPECT_EQ((std::vector<std::string>{"a"}), entries.names);
    }
  }

  if (rebuild) {
    relisting.last_name.clear();
    inject_readdir_reply(client, &relisting, session, diri.get(), right, false,
                         true, {"b"}, perms);
  }
  ASSERT_TRUE(diri->is_complete_and_ordered());
  ASSERT_EQ(complete, cached_listing(diri.get()));
  // Do not reset A: its saved position must survive B's complete rebuild.
  for (unsigned i = 0; i < 2 && !reader.at_end(); ++i) {
    ASSERT_EQ(1, client->_readdir_cache_cb(&reader, ReaddirEntries::one,
                                         &entries, caps, false));
  }
  EXPECT_EQ((std::vector<std::string>{"a", "b"}), entries.names);
  EXPECT_TRUE(reader.at_end());
  ASSERT_EQ(2u, entries.cookies.size());
  EXPECT_EQ(static_cast<off_t>(dir_result_t::END), entries.cookies.back());
}

} // anonymous namespace

TEST_F(TestClient, ReaddirCacheDoesNotEndAfterPartialRebuild) {
  ASSERT_TRUE(client->is_mounted());
  ASSERT_NO_FATAL_FAILURE(
    check_readdir_cache_after_getattr(client, messenger, myperm, false));
  ASSERT_NO_FATAL_FAILURE(
    check_readdir_cache_after_getattr(client, messenger, myperm, true));
}

TEST_F(TestClient, ReaddirLiveCallbackUnlinkRename) {
  const std::string path = "/main_callback_" + std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(path.c_str(), 0755, myperm));
  std::vector<std::string> original;
  for (int i = 0; i < 32; ++i)
    original.push_back("entry_" + std::to_string(i));
  auto cleanup = make_scope_guard([&] {
    for (const auto& name : original)
      client->unlink((path + "/" + name).c_str(), myperm);
    client->unlink((path + "/renamed").c_str(), myperm);
    client->rmdir(path.c_str(), myperm);
  });
  for (const auto& name : original)
    ASSERT_EQ(0, client->mknod((path + "/" + name).c_str(),
                             S_IFREG | 0644, myperm));
  auto list = [&](std::vector<std::string>& names) {
    dir_result_t *handle = nullptr;
    int r = client->opendir(path.c_str(), &handle, myperm);
    if (r < 0)
      return r;
    auto close = make_scope_guard([&] { client->closedir(handle); });
    struct dirent de;
    while ((r = client->readdir_r(handle, &de)) > 0) {
      if (std::string(de.d_name) != "." && std::string(de.d_name) != "..")
        names.emplace_back(de.d_name);
    }
    return r;
  };
  std::vector<std::string> before;
  ASSERT_EQ(0, list(before));
  ASSERT_EQ(original.size(), before.size());
  dir_result_t *reader = nullptr;
  ASSERT_EQ(0, client->opendir(path.c_str(), &reader, myperm));
  auto close_reader = make_scope_guard([&] { client->closedir(reader); });
  {
    std::scoped_lock lock(client->client_lock);
    ASSERT_TRUE(reader->inode->is_complete_and_ordered());
    ASSERT_EQ(before.size(), reader->inode->dir->readdir_cache.size());
    reader->offset = reader->inode->dir->readdir_cache.front()->offset;
  }
  struct Barrier {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool changed = false;
    bool timeout = false;
    std::vector<std::string> seen;
    static int callback(void *p, struct dirent *de, struct ceph_statx *,
                        off_t, Inode *) {
      auto& s = *static_cast<Barrier *>(p);
      std::unique_lock lock(s.mutex);
      s.seen.emplace_back(de->d_name);
      if (!s.entered) {
        s.entered = true;
        s.cv.notify_all();
        if (!s.cv.wait_for(lock, std::chrono::seconds(20), [&] { return s.changed; })) {
          s.timeout = true;
          return -ETIMEDOUT;
        }
      }
      return 0;
    }
  } barrier;
  int mutation_result = -ETIMEDOUT;
  std::vector<std::string> rebuilt;
  std::thread writer([&] {
    {
      std::unique_lock lock(barrier.mutex);
      if (!barrier.cv.wait_for(lock, std::chrono::seconds(10),
                              [&] { return barrier.entered; }))
        return;
    }
    mutation_result = client->unlink((path + "/" + before[0]).c_str(), myperm);
    if (mutation_result == 0)
      mutation_result = client->rename((path + "/" + before[1]).c_str(),
                                       (path + "/renamed").c_str(), myperm, "");
    if (mutation_result == 0)
      mutation_result = list(rebuilt);
    std::lock_guard lock(barrier.mutex);
    barrier.changed = true;
    barrier.cv.notify_all();
  });
  int result;
  {
    std::scoped_lock lock(client->client_lock);
    result = client->_readdir_cache_cb(reader, Barrier::callback, &barrier, 0, false);
  }
  writer.join();
  ASSERT_TRUE(barrier.entered);
  ASSERT_TRUE(barrier.changed);
  ASSERT_FALSE(barrier.timeout);
  ASSERT_EQ(0, mutation_result);
  ASSERT_TRUE(result == 0 || result == -EAGAIN) << result;
  std::set<std::string> expected(original.begin(), original.end());
  expected.erase(before[0]);
  expected.erase(before[1]);
  expected.insert("renamed");
  ASSERT_EQ(expected, std::set<std::string>(rebuilt.begin(), rebuilt.end()));
  ASSERT_EQ(expected.size(), rebuilt.size());
  std::vector<std::string> final;
  ASSERT_EQ(0, list(final));
  ASSERT_EQ(expected, std::set<std::string>(final.begin(), final.end()));
  ASSERT_EQ(expected.size(), final.size());
}

// Failed unlink creates negative dentries on a real MDS directory. Make them
// the LRU victims and prove eviction was reached; repeated stat() alone can
// conclude ENOENT without creating a dentry when FILE_SHARED is held.
TEST_F(TestClient, ReaddirLiveNegativeLruKeepsCache) {
  const std::string path = "/main_negative_" + std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(path.c_str(), 0755, myperm));
  std::set<std::string> expected;
  for (int i = 0; i < 24; ++i) {
    std::string name = "entry_" + std::to_string(i);
    expected.insert(name);
    ASSERT_EQ(0, client->mknod((path + "/" + name).c_str(), S_IFREG | 0644, myperm));
  }
  auto cleanup = make_scope_guard([&] {
    for (const auto& name : expected)
      client->unlink((path + "/" + name).c_str(), myperm);
    client->rmdir(path.c_str(), myperm);
  });
  dir_result_t *reader = nullptr;
  ASSERT_EQ(0, client->opendir(path.c_str(), &reader, myperm));
  auto close_reader = make_scope_guard([&] { client->closedir(reader); });
  struct dirent de;
  int r;
  while ((r = client->readdir_r(reader, &de)) > 0) {}
  ASSERT_EQ(0, r);
  std::unique_lock lock(client->client_lock);
  Inode *diri = reader->inode.get();
  ASSERT_TRUE(diri->is_complete_and_ordered());
  const auto cached = cached_listing(diri);
  ASSERT_EQ(expected.size(), cached.size());
  const auto ordered = diri->dir_ordered_count;
  std::vector<std::string> negatives;
  for (int i = 0; i < 64; ++i) {
    auto name = "missing_" + std::to_string(i);
    lock.unlock();
    r = client->unlink((path + "/" + name).c_str(), myperm);
    lock.lock();
    ASSERT_EQ(-ENOENT, r);
    ASSERT_EQ(1u, diri->dir->dentries.count(name));
    ASSERT_EQ(nullptr, diri->dir->dentries.at(name)->inode);
    negatives.push_back(name);
  }
  ASSERT_EQ(cached, cached_listing(diri));
  for (const auto& name : negatives)
    client->lru.lru_bottouch(diri->dir->dentries.at(name));
  const auto size = client->lru.lru_get_size();
  const auto old_limit = g_ceph_context->_conf->client_cache_size;
  auto restore_limit = make_scope_guard([&] {
    g_ceph_context->_conf.set_val("client_cache_size", std::to_string(old_limit));
  });
  ASSERT_EQ(0, g_ceph_context->_conf.set_val("client_cache_size",
                                           std::to_string(size - negatives.size())));
  client->trim_cache();
  for (const auto& name : negatives)
    EXPECT_EQ(0u, diri->dir->dentries.count(name));
  EXPECT_EQ(size - negatives.size(), client->lru.lru_get_size());
  EXPECT_TRUE(diri->is_complete_and_ordered());
  EXPECT_EQ(ordered, diri->dir_ordered_count);
  ASSERT_EQ(cached, cached_listing(diri));
  reader->offset = diri->dir->readdir_cache.front()->offset;
  std::vector<std::string> seen;
  const auto tid = client->last_tid;
  auto callback = [](void *p, struct dirent *entry, struct ceph_statx *, off_t, Inode *) {
    static_cast<std::vector<std::string> *>(p)->emplace_back(entry->d_name);
    return 0;
  };
  ASSERT_EQ(0, client->_readdir_cache_cb(reader, callback, &seen, 0, false));
  EXPECT_EQ(tid, client->last_tid);
  EXPECT_EQ(expected, std::set<std::string>(seen.begin(), seen.end()));
  EXPECT_EQ(expected.size(), seen.size());
}


TEST(ReaddirRequest, FirstSendZeroSurvivesRedirectAndReplay) {
  MetaRequest request(CEPH_MDS_OP_READDIR);
  EXPECT_FALSE(request.listing_seq_recorded);
  request.record_listing_seq(0);
  EXPECT_TRUE(request.listing_seq_recorded);
  request.mds = -1; // handle_client_forward resets the destination
  request.record_listing_seq(20);
  EXPECT_EQ(0u, request.sent_listing_seq);
  request.got_unsafe = true;
  request.record_listing_seq(30);
  EXPECT_EQ(0u, request.sent_listing_seq);
  request.got_unsafe = false;
}

TEST_F(TestClient, ReaddirFdopendirResetKeepsOwnership) {
  for (bool rewind : {false, true}) {
    int fd = client->open("/", O_RDONLY | O_DIRECTORY, myperm);
    ASSERT_GE(fd, 0);
    dir_result_t *dirp = nullptr;
    ASSERT_EQ(0, client->fdopendir(fd, &dirp, myperm));
    auto close = make_scope_guard([&] { if (dirp) client->closedir(dirp); });
    auto before = dirp->listing_seq;
    client->seekdir(dirp, 0); // also starts a round when already at zero
    EXPECT_GT(dirp->listing_seq, before);
    struct dirent de;
    ASSERT_EQ(1, client->readdir_r(dirp, &de));
    before = dirp->listing_seq;
    if (rewind)
      client->rewinddir(dirp);
    else
      client->seekdir(dirp, 0);
    EXPECT_GT(dirp->listing_seq, before);
    EXPECT_EQ(fd, dirp->fd);
    client->closedir(dirp);
    dirp = nullptr;
    EXPECT_EQ(-EBADF, client->close(fd));
  }
}

TEST_F(TestClient, ReaddirQuotaClearsFreshness) {
  std::unique_lock lock(client->client_lock);
  ReaddirReplyState state;
  auto con = ceph::make_ref<ReaddirTestConnection>(
    client->cct, messenger, state, fake_child_ino("quota"));
  MDSMap saved_map;
  auto session = client->install_readdir_test_session(con, &saved_map);
  auto cleanup = make_scope_guard([&] {
    client->remove_readdir_test_session(session->mds_num, saved_map);
  });
  auto in = make_fake_dir(client, session, myperm);
  in->rstat_seq = 100;
  auto message = ceph::make_message<MClientQuota>();
  message->ino = in->ino;
  message->rstat.rbytes = 2048;
  message->set_src(entity_name_t::MDS(session->mds_num));
  message->set_connection(con);
  lock.unlock();
  client->handle_quota(message);
  lock.lock();
  EXPECT_EQ(2048, in->rstat.rbytes);
  EXPECT_EQ(0u, in->rstat_seq);
}

TEST_F(TestClient, ReaddirSnapdiffKeepsSnapshotCacheOffsets) {
  std::scoped_lock lock(client->client_lock);
  ReaddirTestSession session;
  auto left = make_fake_dir(client, &session, myperm, 100);
  auto right = make_fake_dir(client, &session, myperm, 101);
  auto cleanup = make_scope_guard([&] {
    drop_fake_dir(client, left);
    drop_fake_dir(client, right);
  });
  dir_result_t normal(left, myperm, -1);
  inject_readdir_reply(client, &normal, &session, left.get(), frag_t(), false,
                      true, {"a", "b", "c"}, myperm, S_IFREG | 0644,
                      nullptr, 100);
  ASSERT_TRUE(left->is_complete_and_ordered());
  auto cached = cached_listing(left.get());
  auto pass = left->dir->readdir_pass.id;
  dir_result_t diff(left, myperm, -1);
  inject_readdir_reply(client, &diff, &session, left.get(), frag_t(), false,
                      false, {"c"}, myperm, S_IFREG | 0644, right.get(), 100);
  ASSERT_EQ(1u, diff.buffer.size());
  EXPECT_EQ(2u, dir_result_t::fpos_low(diff.buffer.front().offset));
  EXPECT_EQ(cached, cached_listing(left.get()));
  EXPECT_EQ(pass, left->dir->readdir_pass.id);
  EXPECT_TRUE(left->is_complete_and_ordered());
  inject_readdir_reply(client, &diff, &session, left.get(), frag_t(), false,
                      true, {"removed"}, myperm, S_IFREG | 0644, right.get(), 101);
  ASSERT_EQ(1u, diff.buffer.size());
  EXPECT_EQ(101u, diff.buffer.front().inode->snapid);
  ASSERT_NE(nullptr, right->dir);
  EXPECT_EQ(1u, right->dir->dentries.count("removed"));
  EXPECT_EQ(0u, left->dir->dentries.count("removed"));
  EXPECT_EQ(cached, cached_listing(left.get()));
  EXPECT_EQ(pass, left->dir->readdir_pass.id);
}


TEST_F(TestClient, ReaddirSendRetainsZeroAcrossRedirectAndReplay) {
  std::scoped_lock lock(client->client_lock);
  const auto saved_seq = client->readdir_listing_seq;
  auto restore_seq = make_scope_guard([&] { client->readdir_listing_seq = saved_seq; });
  ReaddirReplyState state;
  auto ino = fake_child_ino("send");
  auto con = ceph::make_ref<ReaddirTestConnection>(client->cct, messenger, state, ino);
  MDSMap saved_map;
  auto session = client->install_readdir_test_session(con, &saved_map);
  auto cleanup = make_scope_guard([&] {
    client->remove_readdir_test_session(session->mds_num, saved_map);
  });
  MetaRequest request(CEPH_MDS_OP_GETATTR);
  request.set_filepath(filepath(ino));
  auto remove = make_scope_guard([&] { request.item.remove_myself(); });
  client->readdir_listing_seq = 0;
  client->send_request(&request, session, false);
  ASSERT_TRUE(state.request);
  EXPECT_TRUE(request.listing_seq_recorded);
  EXPECT_EQ(0u, request.sent_listing_seq);
  state.request.reset();
  request.mds = -1; // handle_client_forward's redirection state
  client->readdir_listing_seq = 40;
  client->send_request(&request, session, false);
  ASSERT_TRUE(state.request);
  EXPECT_EQ(0u, request.sent_listing_seq);
  state.request.reset();
  request.got_unsafe = true;
  request.retry_attempt = 1;
  client->readdir_listing_seq = 80;
  client->send_request(&request, session, false);
  ASSERT_TRUE(state.request);
  EXPECT_EQ(0u, request.sent_listing_seq);
}

TEST_F(TestClient, ReaddirSinglePageRequestBudget) {
  // Run against a single active MDS: every child in READDIR is authoritative.
  const std::string path = "/main_request_budget_" + std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(path.c_str(), 0755, myperm));
  auto cleanup = make_scope_guard([&] {
    for (unsigned i = 0; i < 16; ++i)
      client->rmdir((path + "/d" + std::to_string(i)).c_str(), myperm);
    client->rmdir(path.c_str(), myperm);
  });
  for (unsigned i = 0; i < 16; ++i)
    ASSERT_EQ(0, client->mkdir((path + "/d" + std::to_string(i)).c_str(), 0755, myperm));
  for (unsigned pass = 0; pass < 2; ++pass) {
    dir_result_t *dirp = nullptr;
    ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
    auto close = make_scope_guard([&] { client->closedir(dirp); });
    ceph_tid_t before;
    {
      std::scoped_lock lock(client->client_lock);
      before = client->last_tid;
    }
    struct dirent de;
    unsigned count = 0;
    int rc;
    while ((rc = client->readdir_r(dirp, &de)) > 0)
      ++count;
    ASSERT_EQ(0, rc);
    ASSERT_EQ(18u, count);
    std::scoped_lock lock(client->client_lock);
    EXPECT_EQ(3u, client->last_tid - before)
      << "expected READDIR plus GETATTR for dot and dot-dot";
  }
}
