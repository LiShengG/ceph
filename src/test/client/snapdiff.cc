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

#include "include/scope_guard.h"
#include "test/client/TestClient.h"

TEST_F(TestClient, SnapDiffEntryCountOnRollback) {
  const auto dir = std::string("/snapdiff_rollback_") + std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  dir_result_t* before = nullptr;
  dir_result_t* after = nullptr;
  auto cleanup = make_scope_guard([&] {
    if (before)
      client->closedir(before);
    if (after)
      client->closedir(after);
    client->rmsnap(dir.c_str(), "before", myperm);
    client->rmsnap(dir.c_str(), "after", myperm);
    for (const auto* name : {"a", "b"})
      client->unlink((dir + "/" + name).c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });

  // One entry fits in an 8 KiB reply, but the inode stat of the next
  // differently named entry does not. Its name and lease still fit.
  const std::string xattr(6000, 'x');
  for (const auto* name : {"a", "b"}) {
    const auto path = dir + "/" + name;
    int fd = client->open(path.c_str(), O_CREAT | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
    ASSERT_EQ(0, client->setxattr(path.c_str(), "user.big", xattr.data(),
                                xattr.size(), 0, myperm));
  }
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "before", myperm));
  for (const auto* name : {"a", "b"})
    ASSERT_EQ(0, client->unlink((dir + "/" + name).c_str(), myperm));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "after", myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));

  // Inspect a single reply: automatic pagination could otherwise hide an
  // under-reported count by fetching the omitted entry again.
  ASSERT_EQ(0, client->read_snapdiff_page(before, after->inode->snapid, 8192));
  ASSERT_EQ(1u, before->buffer.size());
  EXPECT_TRUE(before->buffer.front().name == "a" ||
              before->buffer.front().name == "b");
  EXPECT_EQ(before->inode->snapid, before->buffer.front().inode->snapid);
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

  client->rewinddir(before);
  const int r = client->read_snapdiff_page(before, after->inode->snapid, 8192);
  // A successful reply must make progress. A size error can be retried with
  // a larger budget; returning an empty success would loop in readdir.
  EXPECT_TRUE(r == -ERANGE || r == -E2BIG ||
              (r == 0 && before->buffer.size() == 2u))
    << "same-name group returned " << r << " with "
    << before->buffer.size() << " entries";

  client->rewinddir(before);
  ASSERT_EQ(0, client->read_snapdiff_page(before, after->inode->snapid, 16384));
  ASSERT_EQ(2u, before->buffer.size());
  std::set<std::pair<uint64_t, uint64_t>> versions;
  for (const auto& entry : before->buffer) {
    EXPECT_EQ("entry", entry.name);
    versions.emplace(entry.inode->snapid, entry.inode->ino);
  }
  const std::set<std::pair<uint64_t, uint64_t>> expected = {
    {before->inode->snapid, old_ino}, {after->inode->snapid, new_ino}};
  EXPECT_EQ(expected, versions);
  EXPECT_EQ(2u, before->next_offset); // the complete group ends this dirfrag
}

TEST_F(TestClient, SnapDiffSameNameRollbackResume) {
  const auto dir = std::string("/snapdiff_group_resume_") +
                   std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  dir_result_t* before = nullptr;
  dir_result_t* after = nullptr;
  dir_result_t* head = nullptr;
  std::array<std::string, 3> names = {"a", "b", "c"};
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
  // snapdiff orders by hash, not by lexical name. Keep the cheap entry
  // ahead of the replacement pair independently of the filesystem hash.
  std::sort(names.begin(), names.end(), [&](const auto& lhs, const auto& rhs) {
    return std::make_pair(ceph_frag_value(head->inode->hash_dentry_name(lhs)), lhs) <
           std::make_pair(ceph_frag_value(head->inode->hash_dentry_name(rhs)), rhs);
  });
  const auto replacement = dir + "/" + names[1];
  const std::string xattr(6000, 'x');
  for (const auto& name : names) {
    int fd = client->open((dir + "/" + name).c_str(),
                          O_CREAT | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
  }
  ASSERT_EQ(0, client->setxattr(replacement.c_str(), "user.big", xattr.data(),
                              xattr.size(), 0, myperm));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "before", myperm));
  for (const auto& name : names)
    ASSERT_EQ(0, client->unlink((dir + "/" + name).c_str(), myperm));
  int fd = client->open(replacement.c_str(), O_CREAT | O_WRONLY, myperm, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(0, client->close(fd));
  ASSERT_EQ(0, client->setxattr(replacement.c_str(), "user.big", xattr.data(),
                              xattr.size(), 0, myperm));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "after", myperm));
  ASSERT_EQ(0, client->closedir(head));
  head = nullptr;
  // Force both versions to carry their xattrs in the reply, independently
  // of capabilities retained while preparing the replacement inode.
  TearDown();
  SetUp();
  ASSERT_TRUE(client->is_mounted());
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));

  // The cheap prefix and one 6 KiB stat fit. The second version's name
  // and lease fit too, but its inode stat does not: roll back the pair,
  // including its entry count, while keeping the preceding entry.
  ASSERT_EQ(0, client->read_snapdiff_page(before, after->inode->snapid, 8192));
  ASSERT_EQ(1u, before->buffer.size());
  EXPECT_EQ(names[0], before->buffer.front().name);
  EXPECT_EQ(before->inode->snapid, before->buffer.front().inode->snapid);
  ASSERT_EQ(names[0], before->last_name);
  ASSERT_GT(before->next_offset, 2u);
  const auto cursor = before->last_name;

  const int r = client->read_snapdiff_page(before, after->inode->snapid, 8192);
  EXPECT_TRUE(r == -ERANGE || r == -E2BIG ||
              (r == 0 && !before->buffer.empty()))
    << "resumed reply must advance or report an insufficient budget";
  std::set<std::pair<std::string, uint64_t>> entries;
  size_t count = 0;
  if (r == 0) {
    count += before->buffer.size();
    for (const auto& entry : before->buffer)
      entries.emplace(entry.name, entry.inode->snapid);
  }
  if (r == -ERANGE || r == -E2BIG || before->buffer.empty()) {
    EXPECT_EQ(cursor, before->last_name);
  }
  if (r != 0 || before->buffer.empty() || before->next_offset > 2u) {
    ASSERT_EQ(0, client->read_snapdiff_page(before, after->inode->snapid, 16384));
    count += before->buffer.size();
    for (const auto& entry : before->buffer)
      entries.emplace(entry.name, entry.inode->snapid);
  }
  // With an adequate budget, resume from the prefix and return both
  // versions once, then the final deletion. Never skip the rolled-back name.
  EXPECT_EQ(3u, count);
  const std::set<std::pair<std::string, uint64_t>> expected = {
    {names[1], before->inode->snapid}, {names[1], after->inode->snapid},
    {names[2], before->inode->snapid}};
  EXPECT_EQ(expected, entries);
  EXPECT_EQ(names[2], before->last_name);
  EXPECT_EQ(2u, before->next_offset);
}
