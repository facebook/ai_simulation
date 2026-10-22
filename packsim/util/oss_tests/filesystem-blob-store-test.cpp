/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#include <folly/io/IOBuf.h>
#include <folly/testing/TestUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/filesystem-blob-store.h"

namespace packsim::io {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

constexpr std::chrono::hours kTtl{24};

class FilesystemBlobStoreTest : public ::testing::Test {
 protected:
  // `folly::test::TemporaryDirectory::path()` is not a `std::filesystem::path`,
  // so go through `.string()` -- the same conversion the sibling file-io-test
  // uses.
  std::string pathIn(const std::string& relative) const {
    return (tempDir_.path() / relative).string();
  }

  std::string readFileDirectly(const std::string& relative) const {
    std::ifstream in(pathIn(relative));
    return std::string(
        std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }

  // Declared before `store_`, which resolves paths against it. Every case gets
  // a fresh root, so nothing is written outside it and cases cannot see each
  // other's objects.
  folly::test::TemporaryDirectory tempDir_;
  FilesystemBlobStore store_{tempDir_.path().string()};
};

TEST_F(FilesystemBlobStoreTest, ReadReturnsWhatWriteStored) {
  store_.write(
      "my_bucket/config.bin", folly::IOBuf::copyBuffer("hello blob"), kTtl);

  const auto contents = store_.read("my_bucket/config.bin");

  ASSERT_NE(contents, nullptr);
  EXPECT_EQ(contents->moveToFbString().toStdString(), "hello blob");
}

TEST_F(FilesystemBlobStoreTest, BucketBecomesTheFirstPathComponent) {
  store_.write("my_bucket/config.bin", folly::IOBuf::copyBuffer("x"), kTtl);

  // The bucket is not stripped or reinterpreted: the object lands under a
  // directory named for it, so the same string addresses the same object as it
  // would under a remote object-store implementation of BlobStore.
  EXPECT_TRUE(std::filesystem::exists(pathIn("my_bucket/config.bin")));
}

TEST_F(FilesystemBlobStoreTest, WriteCreatesMissingParentDirectories) {
  store_.write(
      "my_bucket/deeply/nested/config.bin",
      folly::IOBuf::copyBuffer("nested"),
      kTtl);

  EXPECT_EQ(readFileDirectly("my_bucket/deeply/nested/config.bin"), "nested");
}

TEST_F(FilesystemBlobStoreTest, WriteOverwritesAnExistingObject) {
  store_.write("my_bucket/config.bin", folly::IOBuf::copyBuffer("first"), kTtl);
  store_.write(
      "my_bucket/config.bin", folly::IOBuf::copyBuffer("second"), kTtl);

  EXPECT_EQ(readFileDirectly("my_bucket/config.bin"), "second");
}

TEST_F(FilesystemBlobStoreTest, WriteFlattensAChainedBuffer) {
  // readConfig hands the store whatever IOBuf the serializer produced, which
  // is commonly a chain rather than one contiguous buffer.
  auto chained = folly::IOBuf::copyBuffer("first-");
  chained->appendToChain(folly::IOBuf::copyBuffer("second"));
  ASSERT_TRUE(chained->isChained());

  store_.write("my_bucket/chained.bin", std::move(chained), kTtl);

  EXPECT_EQ(readFileDirectly("my_bucket/chained.bin"), "first-second");
}

TEST_F(FilesystemBlobStoreTest, ReadThrowsNamingThePathWhenObjectIsMissing) {
  EXPECT_THAT(
      [this] { store_.read("my_bucket/absent.bin"); },
      ThrowsMessage<std::runtime_error>(HasSubstr("my_bucket/absent.bin")));
}

TEST_F(FilesystemBlobStoreTest, WriteRejectsAnAbsolutePath) {
  // Points inside the root, so only its being absolute is rejected.
  const auto absolute = pathIn("my_bucket/config.bin");

  EXPECT_THAT(
      [&] { store_.write(absolute, folly::IOBuf::copyBuffer("x"), kTtl); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(absolute)));
  EXPECT_FALSE(std::filesystem::exists(absolute));
}

TEST_F(FilesystemBlobStoreTest, WriteRejectsAPathThatClimbsOutOfTheRoot) {
  // Rooted one level down, so an escaped write would land inside the temporary
  // directory where the test can look for it.
  FilesystemBlobStore nested{pathIn("root")};

  EXPECT_THAT(
      [&] {
        nested.write(
            "my_bucket/../../escaped.bin", folly::IOBuf::copyBuffer("x"), kTtl);
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("my_bucket/../../escaped.bin")));
  EXPECT_FALSE(std::filesystem::exists(pathIn("escaped.bin")));
  // Rejected before any parent directory is created.
  EXPECT_FALSE(std::filesystem::exists(pathIn("root")));
}

TEST_F(FilesystemBlobStoreTest, ReadRejectsAnAbsolutePath) {
  store_.write("my_bucket/config.bin", folly::IOBuf::copyBuffer("x"), kTtl);
  const auto absolute = pathIn("my_bucket/config.bin");

  EXPECT_THAT(
      [&] { store_.read(absolute); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(absolute)));
}

TEST_F(FilesystemBlobStoreTest, ReadRejectsAPathThatClimbsOutOfTheRoot) {
  // Both the nested root and the file the escaping path names exist, so
  // without the check the read would succeed rather than fail another way.
  store_.write("outside.bin", folly::IOBuf::copyBuffer("x"), kTtl);
  ASSERT_TRUE(std::filesystem::create_directories(pathIn("root")));
  FilesystemBlobStore nested{pathIn("root")};

  EXPECT_THAT(
      [&] { nested.read("../outside.bin"); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("../outside.bin")));
}

TEST_F(FilesystemBlobStoreTest, DotDotThatStaysUnderTheRootIsCollapsed) {
  store_.write(
      "my_bucket/scratch/../config.bin", folly::IOBuf::copyBuffer("in"), kTtl);

  EXPECT_EQ(readFileDirectly("my_bucket/config.bin"), "in");
  EXPECT_FALSE(std::filesystem::exists(pathIn("my_bucket/scratch")));
  EXPECT_EQ(
      store_.read("other/../my_bucket/config.bin")
          ->moveToFbString()
          .toStdString(),
      "in");
}

// `c_str()` ends a name at its first NUL, so without the check this would write
// `my_bucket/evil`, a name with none of the suffixes callers check for.
TEST_F(FilesystemBlobStoreTest, WriteRejectsAPathWithANulByte) {
  const std::string path = std::string("my_bucket/evil") + '\0' + ".json";

  EXPECT_THAT(
      [&] { store_.write(path, folly::IOBuf::copyBuffer("x"), kTtl); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("NUL byte")));
  // Rejected before any parent directory is created.
  EXPECT_FALSE(std::filesystem::exists(pathIn("my_bucket")));
}

TEST_F(FilesystemBlobStoreTest, ReadRejectsAPathWithANulByte) {
  // The object the truncated name would open exists, so without the check the
  // read would succeed rather than fail another way.
  store_.write("my_bucket/evil", folly::IOBuf::copyBuffer("x"), kTtl);
  const std::string path = std::string("my_bucket/evil") + '\0' + ".json";

  EXPECT_THAT(
      [&] { store_.read(path); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("NUL byte")));
}

TEST_F(FilesystemBlobStoreTest, NameThatOnlyStartsWithDotsIsAccepted) {
  store_.write("..my_bucket/config.bin", folly::IOBuf::copyBuffer("x"), kTtl);

  EXPECT_EQ(readFileDirectly("..my_bucket/config.bin"), "x");
}

} // namespace
} // namespace packsim::io
