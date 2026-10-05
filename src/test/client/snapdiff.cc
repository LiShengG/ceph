// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <algorithm>
#include <array>
#include <set>

#include "common/Cond.h"
#include "include/scope_guard.h"
#include "test/client/TestClient.h"

using snapdiff_entries = std::multiset<std::pair<std::string, uint64_t>>;

// Sort names the way readdir_snapdiff returns them: by name hash.
template <size_t N>
static void sort_by_hash(Inode* diri, std::array<std::string, N>& names)
{
  std::sort(names.begin(), names.end(), [&](const auto& lhs, const auto& rhs) {
    return std::make_pair(ceph_frag_value(diri->hash_dentry_name(lhs)), lhs) <
           std::make_pair(ceph_frag_value(diri->hash_dentry_name(rhs)), rhs);
  });
}

TEST_F(TestClient, SnapDiffOversizedSameNameGroup) {
  const auto dir = std::string("/snapdiff_oversized_group_") +
                   std::to_string(getpid());
  const auto path = dir + "/entry";
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  dir_result_t* before = nullptr;
  dir_result_t* empty = nullptr;
  dir_result_t* after = nullptr;
  auto cleanup = make_scope_guard([&] {
    for (auto* dirp : {before, empty, after}) {
      if (dirp)
        client->closedir(dirp);
    }
    for (const auto* snap : {"before", "empty", "after"})
      client->rmsnap(dir.c_str(), snap, myperm);
    client->unlink(path.c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });

  const std::string xattr(6000, 'x');
  for (const auto* snap : {"before", "after"}) {
    int fd = client->open(path.c_str(), O_CREAT | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
    ASSERT_EQ(0, client->setxattr(path.c_str(), "user.big", xattr.data(),
                                xattr.size(), 0, myperm));
    ASSERT_EQ(0, client->mksnap(dir.c_str(), snap, myperm));
    if (std::string(snap) == "before") {
      ASSERT_EQ(0, client->unlink(path.c_str(), myperm));
      ASSERT_EQ(0, client->mksnap(dir.c_str(), "empty", myperm));
    }
  }
  // Drop the writer's caps before reading. A cached xattr version can
  // otherwise omit the large xattr blob and let both versions fit.
  TearDown();
  SetUp();
  ASSERT_TRUE(client->is_mounted());
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/empty").c_str(), &empty, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));

  // Establish that each version fits by itself, rather than assuming an
  // inode stat's wire size. The pair needs more than the 8 KiB budget.
  ASSERT_EQ(0, client->read_snapdiff_page(before, empty->inode->snapid, 8192));
  ASSERT_EQ(1u, before->buffer.size());
  const auto old_ino = before->buffer.front().inode->ino;
  EXPECT_EQ("entry", before->buffer.front().name);
  EXPECT_EQ(before->inode->snapid, before->buffer.front().inode->snapid);
  EXPECT_EQ(2u, before->next_offset);
  ASSERT_EQ(0, client->read_snapdiff_page(empty, after->inode->snapid, 8192));
  ASSERT_EQ(1u, empty->buffer.size());
  const auto new_ino = empty->buffer.front().inode->ino;
  ASSERT_NE(old_ino, new_ino);
  EXPECT_EQ("entry", empty->buffer.front().name);
  EXPECT_EQ(after->inode->snapid, empty->buffer.front().inode->snapid);
  EXPECT_EQ(2u, empty->next_offset);

  // A single entry that exceeds the budget on its own is still returned.
  client->rewinddir(before);
  ASSERT_EQ(0, client->read_snapdiff_page(before, empty->inode->snapid, 1024));
  ASSERT_EQ(1u, before->buffer.size());
  EXPECT_EQ(old_ino, before->buffer.front().inode->ino);
  EXPECT_EQ(2u, before->next_offset);

  // The same-name group can't be split and is the first one in the reply,
  // so rolling it back would leave nothing to send. A successful reply
  // without entries would make readdir skip the dirfrag or loop forever.
  // Send the whole group even though it exceeds the budget.
  for (const unsigned budget : {8192u, 1024u}) {
    SCOPED_TRACE(budget);
    client->rewinddir(before);
    ASSERT_EQ(0, client->read_snapdiff_page(before, after->inode->snapid, budget));
    ASSERT_EQ(2u, before->buffer.size());
    std::set<std::pair<uint64_t, uint64_t>> versions;
    for (const auto& entry : before->buffer) {
      EXPECT_EQ("entry", entry.name);
      versions.emplace(entry.inode->snapid, entry.inode->ino);
    }
    const std::set<std::pair<uint64_t, uint64_t>> expected = {
      {before->inode->snapid, old_ino}, {after->inode->snapid, new_ino}};
    EXPECT_EQ(expected, versions);
    EXPECT_EQ("entry", before->last_name);
    EXPECT_EQ(2u, before->next_offset); // the complete group ends this dirfrag
  }
}

TEST_F(TestClient, SnapDiffSameNameRollbackResume) {
  const auto dir = std::string("/snapdiff_group_resume_") +
                   std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  dir_result_t* before = nullptr;
  dir_result_t* after = nullptr;
  dir_result_t* head = nullptr;
  std::array<std::string, 4> names = {"a", "b", "c", "d"};
  auto cleanup = make_scope_guard([&] {
    for (auto* dirp : {before, after, head}) {
      if (dirp)
        client->closedir(dirp);
    }
    client->rmsnap(dir.c_str(), "before", myperm);
    client->rmsnap(dir.c_str(), "after", myperm);
    for (const auto& name : names)
      client->unlink((dir + "/" + name).c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });
  ASSERT_EQ(0, client->opendir(dir.c_str(), &head, myperm));
  // snapdiff orders by hash, not by lexical name. In that order:
  // names[0] is deleted, names[1] is replaced by a new inode with the same
  // name, names[2] is created and names[3] is unchanged. The unchanged
  // entry keeps every reply that stops at names[2] from being the last
  // one in the dirfrag.
  sort_by_hash(head->inode.get(), names);
  ASSERT_EQ(0, client->closedir(head));
  head = nullptr;
  const auto replacement = dir + "/" + names[1];
  const std::string xattr(6000, 'x');
  auto create = [&](const std::string& name) {
    int fd = client->open((dir + "/" + name).c_str(),
                          O_CREAT | O_EXCL | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
  };
  for (const auto i : {0, 1, 3})
    ASSERT_NO_FATAL_FAILURE(create(names[i]));
  ASSERT_EQ(0, client->setxattr(replacement.c_str(), "user.big", xattr.data(),
                              xattr.size(), 0, myperm));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "before", myperm));
  for (const auto i : {0, 1})
    ASSERT_EQ(0, client->unlink((dir + "/" + names[i]).c_str(), myperm));
  ASSERT_NO_FATAL_FAILURE(create(names[1]));
  ASSERT_EQ(0, client->setxattr(replacement.c_str(), "user.big", xattr.data(),
                              xattr.size(), 0, myperm));
  ASSERT_NO_FATAL_FAILURE(create(names[2]));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "after", myperm));
  // Force both versions to carry their xattrs in the reply, independently
  // of capabilities retained while preparing the replacement inode.
  TearDown();
  SetUp();
  ASSERT_TRUE(client->is_mounted());
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));
  const auto snap_before = before->inode->snapid;
  const auto snap_after = after->inode->snapid;

  // The cheap prefix and one 6 KiB stat fit. The second version's name
  // and lease fit too, but its inode stat does not: roll back the pair,
  // including its entry count, while keeping the preceding entry.
  ASSERT_EQ(0, client->read_snapdiff_page(before, snap_after, 8192));
  ASSERT_EQ(1u, before->buffer.size());
  EXPECT_EQ(names[0], before->buffer.front().name);
  EXPECT_EQ(snap_before, before->buffer.front().inode->snapid);
  ASSERT_EQ(names[0], before->last_name);
  ASSERT_GT(before->next_offset, 2u);

  // Resuming, the pair is the first group of the reply. It is sent whole
  // although it exceeds the budget, and the reply ends right after it.
  ASSERT_EQ(0, client->read_snapdiff_page(before, snap_after, 8192));
  snapdiff_entries entries;
  for (const auto& entry : before->buffer)
    entries.emplace(entry.name, entry.inode->snapid);
  snapdiff_entries expected = {{names[1], snap_before}, {names[1], snap_after}};
  EXPECT_EQ(expected, entries);
  ASSERT_EQ(names[1], before->last_name);
  ASSERT_GT(before->next_offset, 2u);

  // Pagination continues after the oversized group.
  ASSERT_EQ(0, client->read_snapdiff_page(before, snap_after, 8192));
  entries.clear();
  for (const auto& entry : before->buffer)
    entries.emplace(entry.name, entry.inode->snapid);
  expected = {{names[2], snap_after}};
  EXPECT_EQ(expected, entries);
  EXPECT_EQ(names[2], before->last_name);
  EXPECT_EQ(2u, before->next_offset);
}

// Go through Client::readdir_snapdiff() with the default reply budget of
// (512 KiB + max_xattr_size). A replaced inode with xattrs close to
// max_xattr_size makes its same-name group exceed it. The MDS used to roll
// such a group back to an empty reply, making the client silently skip
// the rest of the dirfrag (on its first page) or request the same position
// forever (later on).
TEST_F(TestClient, SnapDiffOversizedSameNameGroupReaddir) {
  const auto dir = std::string("/snapdiff_oversized_readdir_") +
                   std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  const auto [fs_name, orig_max_xattr_size] =
    client->get_fs_name_and_max_xattr_size();
  constexpr uint64_t max_xattr_size = 1 << 20;
  auto set_max_xattr_size = [&](uint64_t size) {
    bufferlist outbl;
    std::string outs;
    C_SaferCond cond;
    mc->start_mon_command(
      {"{\"prefix\": \"fs set\", \"fs_name\": \"" + fs_name +
       "\", \"var\": \"max_xattr_size\", \"val\": \"" +
       std::to_string(size) + "\"}"},
      {}, &outbl, &outs, &cond);
    return cond.wait();
  };
  dir_result_t* before = nullptr;
  dir_result_t* after = nullptr;
  dir_result_t* head = nullptr;
  std::array<std::string, 4> names = {"a", "b", "c", "d"};
  auto cleanup = make_scope_guard([&] {
    for (auto* dirp : {before, after, head}) {
      if (dirp)
        client->closedir(dirp);
    }
    client->rmsnap(dir.c_str(), "before", myperm);
    client->rmsnap(dir.c_str(), "after", myperm);
    for (const auto& name : names)
      client->unlink((dir + "/" + name).c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
    EXPECT_EQ(0, set_max_xattr_size(orig_max_xattr_size));
  });
  ASSERT_LT(orig_max_xattr_size, max_xattr_size);
  ASSERT_EQ(0, set_max_xattr_size(max_xattr_size));

  ASSERT_EQ(0, client->opendir(dir.c_str(), &head, myperm));
  // In hash order: names[0] and names[2] are replaced by new inodes with
  // the same name, names[1] is created and names[3] is unchanged. The
  // replacement names[0] is the first group of the first reply, names[2]
  // the first group of a later one.
  sort_by_hash(head->inode.get(), names);
  ASSERT_EQ(0, client->closedir(head));
  head = nullptr;

  // Each version fits the budget by itself, but not both of them.
  const std::string xattr(max_xattr_size - 64, 'x');
  auto create = [&](const std::string& name, bool big) {
    const auto path = dir + "/" + name;
    int fd = client->open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
    if (!big)
      return;
    // The MDS may see the new max_xattr_size after the client does.
    int r;
    for (int i = 0; i < 60; ++i) {
      r = client->setxattr(path.c_str(), "user.big", xattr.data(),
                           xattr.size(), 0, myperm);
      if (r != -ENOSPC)
        break;
      sleep(1);
    }
    ASSERT_EQ(0, r);
  };
  for (const auto i : {0, 2})
    ASSERT_NO_FATAL_FAILURE(create(names[i], true));
  ASSERT_NO_FATAL_FAILURE(create(names[3], false));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "before", myperm));
  for (const auto i : {0, 2}) {
    ASSERT_EQ(0, client->unlink((dir + "/" + names[i]).c_str(), myperm));
    ASSERT_NO_FATAL_FAILURE(create(names[i], true));
  }
  ASSERT_NO_FATAL_FAILURE(create(names[1], false));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "after", myperm));
  // Make the replies carry the xattrs of both versions.
  TearDown();
  SetUp();
  ASSERT_TRUE(client->is_mounted());
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));
  const auto snap_before = before->inode->snapid;
  const auto snap_after = after->inode->snapid;

  for (const bool reverse : {false, true}) {
    SCOPED_TRACE(reverse ? "after to before" : "before to after");
    auto* reader = reverse ? after : before;
    const auto other_snap = reverse ? snap_before : snap_after;
    client->rewinddir(reader);
    snapdiff_entries entries;
    struct dirent de;
    snapid_t snap;
    int r;
    while ((r = client->readdir_snapdiff(reader, other_snap, 0, &de, &snap)) > 0) {
      if (strcmp(de.d_name, ".") == 0 || strcmp(de.d_name, "..") == 0)
        continue;
      entries.emplace(de.d_name, uint64_t(snap));
      ASSERT_LE(entries.size(), 5u);
    }
    ASSERT_EQ(0, r);
    const snapdiff_entries expected = {
      {names[0], snap_before}, {names[0], snap_after},
      {names[1], snap_after},
      {names[2], snap_before}, {names[2], snap_after}};
    EXPECT_EQ(expected, entries);
  }
}
