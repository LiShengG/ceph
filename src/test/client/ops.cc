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
#include "client/Inode.h"
#include "client/MetaSession.h"
#include "include/scope_guard.h"
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
