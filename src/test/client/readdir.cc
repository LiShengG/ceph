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
#include <string>
#include <vector>

#include "include/scope_guard.h"
#include "test/client/TestClient.h"

using pages_t = std::vector<std::vector<std::string>>;

// Read @dirp one reply at a time with the byte budget @max_bytes, as
// the kernel client does. Every reply must make progress.
static void read_pages(ClientScaffold* client, dir_result_t* dirp,
                       unsigned max_bytes, pages_t* pages)
{
  // don't spin on a reply that makes no progress
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(0, client->read_dir_page(dirp, max_bytes));
    std::vector<std::string> names;
    for (const auto& entry : dirp->buffer)
      names.push_back(entry.name);
    ASSERT_FALSE(names.empty()) << "reply " << i << " has no entries";
    pages->push_back(std::move(names));
    // the reply ends the dirfrag
    if (dirp->next_offset == 2)
      return;
  }
  FAIL() << "the dirfrag did not end";
}

static std::vector<std::string> flatten(const pages_t& pages)
{
  std::vector<std::string> names;
  for (const auto& page : pages)
    names.insert(names.end(), page.begin(), page.end());
  return names;
}

TEST_F(TestClient, ReaddirFirstEntryOverBudget) {
  const auto dir = std::string("/readdir_over_budget_") +
                   std::to_string(getpid());
  std::array<std::string, 5> names = {"a", "b", "c", "d", "e"};
  dir_result_t* dirp = nullptr;
  auto cleanup = make_scope_guard([&] {
    if (dirp)
      client->closedir(dirp);
    for (const auto& name : names)
      client->unlink((dir + "/" + name).c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  for (const auto& name : names) {
    int fd = client->open((dir + "/" + name).c_str(),
                          O_CREAT | O_EXCL | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
  }
  // sort the names the way readdir returns them: by name hash
  ASSERT_EQ(0, client->opendir(dir.c_str(), &dirp, myperm));
  Inode* diri = dirp->inode.get();
  std::sort(names.begin(), names.end(), [&](const auto& lhs, const auto& rhs) {
    return std::make_pair(ceph_frag_value(diri->hash_dentry_name(lhs)), lhs) <
           std::make_pair(ceph_frag_value(diri->hash_dentry_name(rhs)), rhs);
  });
  ASSERT_EQ(0, client->closedir(dirp));
  dirp = nullptr;

  // The inode stat of the entry with the xattr exceeds the budget. Put
  // it at the start of the dirfrag, then in the middle.
  const std::string xattr(6000, 'x');
  for (const size_t big : {0, 2}) {
    SCOPED_TRACE(big);
    const auto path = dir + "/" + names[big];
    ASSERT_EQ(0, client->setxattr(path.c_str(), "user.big", xattr.data(),
                                  xattr.size(), 0, myperm));
    // Remount so that the replies carry the xattrs.
    TearDown();
    SetUp();
    ASSERT_TRUE(client->is_mounted());
    ASSERT_EQ(0, client->opendir(dir.c_str(), &dirp, myperm));
    pages_t pages;
    // on failure, still try the other position
    EXPECT_NO_FATAL_FAILURE(read_pages(client, dirp, 4096, &pages));
    ASSERT_EQ(0, client->closedir(dirp));
    dirp = nullptr;
    // the entry is sent alone, beyond the budget
    const std::vector<std::string> alone = {names[big]};
    EXPECT_NE(pages.end(), std::find(pages.begin(), pages.end(), alone));
    const std::vector<std::string> expected(names.begin(), names.end());
    EXPECT_EQ(expected, flatten(pages));
    ASSERT_EQ(0, client->removexattr(path.c_str(), "user.big", myperm));
  }
}

TEST_F(TestClient, LssnapFirstEntryOverBudget) {
  const auto dir = std::string("/lssnap_over_budget_") +
                   std::to_string(getpid());
  const std::vector<std::string> snaps = {"s1", "s2", "s3"};
  dir_result_t* dirp = nullptr;
  auto cleanup = make_scope_guard([&] {
    if (dirp)
      client->closedir(dirp);
    for (const auto& snap : snaps)
      client->rmsnap(dir.c_str(), snap.c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  for (const auto& snap : snaps)
    ASSERT_EQ(0, client->mksnap(dir.c_str(), snap.c_str(), myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap").c_str(), &dirp, myperm));
  // The snapshots carry no xattrs here, since the client already has
  // those of the directory. Use a budget smaller than any entry.
  pages_t pages;
  ASSERT_NO_FATAL_FAILURE(read_pages(client, dirp, 1, &pages));
  // one snapshot per reply
  const pages_t expected = {{snaps[0]}, {snaps[1]}, {snaps[2]}};
  EXPECT_EQ(expected, pages);
}
