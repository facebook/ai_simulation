/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fmt/format.h>
#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>
#include <folly/json/json.h>
#include <folly/testing/TestUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/util/blob-store.h"
#include "packsim/util/file-io.h"

namespace packsim::io {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// A blob store that keeps objects in memory. The real store reaches the
// network, which a unit test that is not allowed to reach the network cannot
// do; injecting this one covers the routing and serialization around the store
// without asserting anything about the store itself.
class FakeBlobStore : public IBlobStore {
 public:
  std::unique_ptr<folly::IOBuf> read(const std::string& path) override {
    const auto it = objects_.find(path);
    if (it == objects_.end()) {
      throw std::runtime_error(fmt::format("no such object: {}", path));
    }
    return it->second->clone();
  }

  void write(
      const std::string& path,
      std::unique_ptr<folly::IOBuf> contents,
      std::chrono::hours ttl) override {
    lastTtl_ = ttl;
    objects_[path] = std::move(contents);
  }

  bool contains(const std::string& path) const {
    return objects_.contains(path);
  }

  std::chrono::hours lastTtl() const {
    return lastTtl_;
  }

 private:
  std::map<std::string, std::unique_ptr<folly::IOBuf>> objects_;
  std::chrono::hours lastTtl_{0};
};

class FileIoTest : public ::testing::Test {
 protected:
  std::string pathIn(const std::string& relative) const {
    return (tempDir_.path() / relative).string();
  }

  void writeFile(const std::string& path) const {
    std::ofstream out(path);
    out << "placeholder";
  }

  // Create a symlink whose target is never created, and assert it really is
  // dangling so a test built on it cannot pass for the wrong reason.
  std::string danglingSymlinkAt(const std::string& relative) const {
    const auto link = pathIn(relative);
    std::filesystem::create_symlink(pathIn("never-created.json"), link);
    EXPECT_FALSE(std::filesystem::exists(link))
        << "precondition: the link must be dangling";
    return link;
  }

  folly::test::TemporaryDirectory tempDir_;
  FakeBlobStore blobStore_;
};

// An empty output name means "no file output was requested", which is a
// success rather than something to create a directory for.
TEST_F(FileIoTest, EmptyOutputNameIsAccepted) {
  EXPECT_TRUE(
      checkOutputPathAndCreateDir(/*outputFileName=*/"", /*overwrite=*/false));
}

// An existing result file is left alone unless the caller allows overwriting
// it.
TEST_F(FileIoTest, ExistingFileIsRejectedWithoutOverwrite) {
  const auto path = pathIn("results.json");
  writeFile(path);

  EXPECT_FALSE(checkOutputPathAndCreateDir(path, /*overwrite=*/false));
}

TEST_F(FileIoTest, ExistingFileIsAcceptedWithOverwrite) {
  const auto path = pathIn("results.json");
  writeFile(path);

  EXPECT_TRUE(checkOutputPathAndCreateDir(path, /*overwrite=*/true));
}

// A path whose parent directories do not exist yet is accepted, and the
// directories are created so the later write cannot fail on a missing parent.
TEST_F(FileIoTest, MissingParentDirectoriesAreCreated) {
  const auto path = pathIn("nested/deeper/results.json");

  EXPECT_TRUE(checkOutputPathAndCreateDir(path, /*overwrite=*/false));
  EXPECT_TRUE(std::filesystem::is_directory(pathIn("nested/deeper")));
}

// A dangling symlink occupies the name even though nothing is behind it, so
// writing to it would clobber whatever the link is later pointed at. The
// existence check deliberately does not follow symlinks; a check that did
// would report "no such file" for these two and let the write through.
TEST_F(FileIoTest, DanglingSymlinkBlocksWriteWithoutOverwrite) {
  const auto link = danglingSymlinkAt("results.json");

  EXPECT_FALSE(checkOutputPathAndCreateDir(link, /*overwrite=*/false));
}

TEST_F(FileIoTest, DanglingSymlinkAllowsWriteWithOverwrite) {
  const auto link = danglingSymlinkAt("results.json");

  EXPECT_TRUE(checkOutputPathAndCreateDir(link, /*overwrite=*/true));
}

// readConfig takes a file path and a blob-store path and picks one; supplying
// both is ambiguous and must fail loudly rather than silently preferring one.
TEST_F(FileIoTest, ReadConfigRejectsBothSourcesAtOnce) {
  packsim::thrift::Simulation cfg;

  EXPECT_THAT(
      [&]() {
        readConfig(&blobStore_, "/tmp/some-file.json", "some/blob/path", cfg);
      },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Cannot specify both file and blob path")));
}

// Supplying neither source is not an error: it reports "nothing was loaded" so
// the caller can fall back to its own defaults.
TEST_F(FileIoTest, ReadConfigReportsNothingLoadedWhenNoSourceGiven) {
  packsim::thrift::Simulation cfg;

  EXPECT_FALSE(readConfig(&blobStore_, /*filePath=*/"", /*blobPath=*/"", cfg));
}

// A config written to the blob store must come back out of it unchanged. This
// is the round trip the replay flow depends on, and before the store became an
// injected interface there was no way to exercise it without the network.
TEST_F(FileIoTest, ConfigRoundTripsThroughTheBlobStore) {
  constexpr auto kPath = "bucket/tree/cfg.json";
  packsim::thrift::Simulation written;
  written.snapshotIntervalNs() = 4242;

  writeConfigToBlobStore(blobStore_, kPath, written);
  ASSERT_TRUE(blobStore_.contains(kPath));

  packsim::thrift::Simulation read;
  EXPECT_TRUE(readConfig(&blobStore_, /*filePath=*/"", kPath, read));
  EXPECT_EQ(*read.snapshotIntervalNs(), 4242);
}

// The 60-day retention is the reason results survive long enough to be
// replayed, so it is worth pinning rather than leaving to the store's default.
TEST_F(FileIoTest, WrittenConfigCarriesTheRetentionPeriod) {
  packsim::thrift::Simulation cfg;

  writeConfigToBlobStore(blobStore_, "bucket/tree/cfg.json", cfg);

  EXPECT_EQ(blobStore_.lastTtl(), std::chrono::hours(24 * 60));
}

// A blob path with no store behind it is a caller error, not something to
// swallow: the config the caller asked for would silently never load.
TEST_F(FileIoTest, ReadConfigWithoutABlobStoreThrows) {
  packsim::thrift::Simulation cfg;

  EXPECT_THAT(
      [&cfg]() {
        readConfig(
            /*blobStore=*/nullptr,
            /*filePath=*/"",
            "bucket/tree/cfg.json",
            cfg);
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("no blob store")));
}

// A result carrying both the fields the simplified output keeps and one it
// drops, so a test can tell the two forms apart.
packsim::thrift::SimulationResult makeResult() {
  packsim::thrift::SimulationResult result;
  result.buildRevision() = "rev-under-test";
  result.config()->jobInfo()->sweepId() = "sweep-7";
  packsim::thrift::WorkloadResult workload;
  workload.name() = "w";
  workload.workloadId() = 3;
  workload.startTimeNs() = 100;
  workload.endTimeNs() = 900;
  workload.runTimeNs() = 800;
  result.workloads()->emplace("w", std::move(workload));
  return result;
}

std::string readWholeFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(
      std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// Both names empty is a run that asked for no output, not an error.
TEST_F(FileIoTest, PrepareAcceptsNoDestinations) {
  EXPECT_NO_THROW(
      prepareOutputDestinations(/*outputFileName=*/"", /*blobOutput=*/""));
}

TEST_F(FileIoTest, PrepareRejectsAnUnsupportedOutputExtension) {
  const auto path = pathIn("results.txt");

  EXPECT_THAT(
      [&path] { prepareOutputDestinations(path, /*blobOutput=*/""); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr(fmt::format("Unsupported output file type: {}", path))));
}

// The blob name is checked before the file's directory is created, so a bad
// blob name leaves nothing behind either.
TEST_F(FileIoTest, PrepareRejectsAnUnsupportedBlobExtension) {
  EXPECT_THAT(
      [this] {
        prepareOutputDestinations(
            pathIn("nested/results.json"), "bucket/tree/results.txt");
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Unsupported output file type: bucket/tree/results.txt")));
  EXPECT_FALSE(std::filesystem::exists(pathIn("nested")));
}

TEST_F(FileIoTest, PrepareCreatesNothingForARejectedName) {
  EXPECT_THAT(
      [this] {
        prepareOutputDestinations(
            pathIn("nested/results.txt"), /*blobOutput=*/"");
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Unsupported output file type")));
  EXPECT_FALSE(std::filesystem::exists(pathIn("nested")));
}

// The shell does not expand "~" after "--output_file=", so creating the parent
// directories would make a directory named "~" in the working directory.
TEST_F(FileIoTest, PrepareRejectsALeadingTilde) {
  EXPECT_THAT(
      [] { prepareOutputDestinations("~/results.json", /*blobOutput=*/""); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("~/results.json")));
}

// open() on a directory fails with EISDIR, so without this check the run would
// finish and only then lose its result.
TEST_F(FileIoTest, PrepareRejectsAnOutputPathThatIsADirectory) {
  const auto path = pathIn("results.json");
  std::filesystem::create_directory(path);

  EXPECT_THAT(
      [&path] { prepareOutputDestinations(path, /*blobOutput=*/""); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr(fmt::format("Output file {} is a directory", path))));
}

// open() follows the link, so a link to a directory fails the same way.
TEST_F(FileIoTest, PrepareRejectsASymlinkToADirectory) {
  const auto target = pathIn("real-dir");
  std::filesystem::create_directory(target);
  const auto link = pathIn("results.json");
  std::filesystem::create_directory_symlink(target, link);

  EXPECT_THAT(
      [&link] { prepareOutputDestinations(link, /*blobOutput=*/""); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr(fmt::format("Output file {} is a directory", link))));
}

// A parent that exists as a regular file cannot become a directory.
TEST_F(FileIoTest, PrepareReportsAParentThatCannotBeCreated) {
  writeFile(pathIn("blocker"));
  const auto path = pathIn("blocker/results.json");

  EXPECT_THAT(
      [&path] { prepareOutputDestinations(path, /*blobOutput=*/""); },
      ThrowsMessage<std::runtime_error>(HasSubstr(
          fmt::format(
              "Cannot create the directory for output file {}", path))));
}

TEST_F(FileIoTest, PrepareCreatesMissingParentDirectories) {
  prepareOutputDestinations(
      pathIn("nested/deeper/results.json"), /*blobOutput=*/"");

  EXPECT_TRUE(std::filesystem::is_directory(pathIn("nested/deeper")));
}

// The existing result is replaced by the write after the run, not truncated by
// the check before it, so a run that fails keeps the previous result.
TEST_F(FileIoTest, PrepareLeavesAnExistingOutputFileInPlace) {
  const auto path = pathIn("results.json");
  writeFile(path);

  EXPECT_NO_THROW(prepareOutputDestinations(path, /*blobOutput=*/""));
  EXPECT_EQ(readWholeFile(path), "placeholder");
}

class WriteOutputFileTest : public FileIoTest {
 protected:
  packsim::thrift::SimulationResult result_{makeResult()};
};

// Neither destination requested is a no-op rather than an empty write.
TEST_F(WriteOutputFileTest, NoDestinationWritesNothing) {
  writeOutputFile(
      &blobStore_,
      /*outputFileName=*/"",
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/false);

  EXPECT_TRUE(std::filesystem::is_empty(tempDir_.path().string()));
}

TEST_F(WriteOutputFileTest, JsonOutputIsWrittenAsReadableJson) {
  const auto path = pathIn("results.json");

  writeOutputFile(
      &blobStore_,
      path,
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/false);

  ASSERT_TRUE(std::filesystem::exists(path));
  EXPECT_THAT(readWholeFile(path), HasSubstr("rev-under-test"));
}

// writeOutputFile creates no directories (prepareOutputDestinations does,
// before the run), so a missing one fails the open, and the error names the
// file.
TEST_F(WriteOutputFileTest, AnOpenFailureNamesTheFile) {
  const auto path = pathIn("missing/results.json");

  EXPECT_THAT(
      [&] {
        writeOutputFile(
            &blobStore_,
            path,
            /*blobOutput=*/"",
            result_,
            /*addSimplifiedOutput=*/false);
      },
      ThrowsMessage<std::system_error>(
          HasSubstr(fmt::format("Failed to open output file {}", path))));
}

// `nicQpnPairMap` is keyed by structs, the shape Simple JSON cannot write as
// valid JSON. The file must still parse, with each struct-keyed map written as
// a list of {"key", "value"} objects.
TEST_F(WriteOutputFileTest, StructKeyedMapsAreWrittenAsValidJson) {
  const auto path = pathIn("results.json");
  packsim::thrift::RankPair ranks;
  ranks.srcRank() = 0;
  ranks.dstRank() = 1;
  packsim::thrift::NicPair nics;
  nics.srcNicIdx() = 0;
  nics.dstNicIdx() = 2;
  packsim::workload::thrift::QPNPair qpns;
  qpns.local() = 7;
  qpns.remote() = 9;
  result_.workloads()
      ->at("w")
      .workCfg()
      ->nicQpnPairMap()[ranks][nics]
      .emplace_back(qpns);

  writeOutputFile(
      &blobStore_,
      path,
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/false);

  const auto parsed = folly::parseJson(readWholeFile(path));
  EXPECT_EQ(
      parsed["workloads"]["w"]["workCfg"]["nicQpnPairMap"],
      folly::parseJson(R"([{
        "key": {"srcRank": 0, "dstRank": 1},
        "value": [{
          "key": {"srcNicIdx": 0, "dstNicIdx": 2},
          "value": [{"local": 7, "remote": 9}]
        }]
      }])"));
}

// The extension drives compression, so a .gz name must produce a gzip member
// rather than plain JSON with a misleading name.
TEST_F(WriteOutputFileTest, GzipExtensionCompressesTheOutput) {
  const auto path = pathIn("results.json.gz");

  writeOutputFile(
      &blobStore_,
      path,
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/false);

  const auto contents = readWholeFile(path);
  ASSERT_GE(contents.size(), 2u);
  EXPECT_EQ(contents[0], '\x1f');
  EXPECT_EQ(contents[1], '\x8b');
  EXPECT_THAT(contents, ::testing::Not(HasSubstr("rev-under-test")));
}

// Compression is applied exactly once, by serialize(). Checking the gzip magic
// bytes is not enough to pin that: a buffer compressed twice still starts 1f 8b
// and still hides the payload, so only decompressing once and finding the
// payload distinguishes the two.
TEST_F(WriteOutputFileTest, GzipOutputDecompressesBackToThePayload) {
  const auto path = pathIn("results.json.gz");

  writeOutputFile(
      &blobStore_,
      path,
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/false);

  const auto contents = readWholeFile(path);
  const auto decompressed =
      folly::compression::getCodec(folly::compression::CodecType::GZIP)
          ->uncompress(folly::IOBuf::copyBuffer(contents).get());
  ASSERT_NE(decompressed, nullptr);
  EXPECT_THAT(
      decompressed->moveToFbString().toStdString(),
      HasSubstr("rev-under-test"));
}

// The same single-compression guarantee on the blob path, which reaches the
// store through a different branch of writeOutputFile than the local file.
TEST_F(WriteOutputFileTest, GzipBlobUploadDecompressesBackToThePayload) {
  constexpr auto kBlobPath = "bucket/tree/results.json.gz";

  writeOutputFile(
      &blobStore_,
      /*outputFileName=*/"",
      kBlobPath,
      result_,
      /*addSimplifiedOutput=*/false);

  const auto uploaded = blobStore_.read(kBlobPath);
  ASSERT_NE(uploaded, nullptr) << "nothing was uploaded to " << kBlobPath;
  const auto decompressed =
      folly::compression::getCodec(folly::compression::CodecType::GZIP)
          ->uncompress(uploaded.get());
  ASSERT_NE(decompressed, nullptr);
  EXPECT_THAT(
      decompressed->moveToFbString().toStdString(),
      HasSubstr("rev-under-test"));
}

// The simplified output is a second file beside the first, carrying only the
// job info and per-workload timings, for readers that do not need the full
// result.
TEST_F(WriteOutputFileTest, SimplifiedOutputIsWrittenBesideTheFullOne) {
  const auto path = pathIn("results.json");

  writeOutputFile(
      &blobStore_,
      path,
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/true);

  const auto simplified = pathIn("results_simplified.json");
  ASSERT_TRUE(std::filesystem::exists(simplified));
  const auto contents = readWholeFile(simplified);
  EXPECT_THAT(contents, HasSubstr("sweep-7"));
  EXPECT_THAT(contents, HasSubstr("\"w\""));
  EXPECT_THAT(contents, ::testing::Not(HasSubstr("rev-under-test")));
}

// The "_simplified" marker goes before the extension, not after the whole name,
// so a compressed result keeps its .gz suffix and stays decompressible.
TEST_F(WriteOutputFileTest, SimplifiedNameKeepsTheGzipSuffixLast) {
  writeOutputFile(
      &blobStore_,
      pathIn("results.json.gz"),
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/true);

  EXPECT_TRUE(std::filesystem::exists(pathIn("results_simplified.json.gz")));
}

// The marker goes before the last extension of the uncompressed name, so a
// name carrying dots of its own keeps every one of them ahead of the marker.
TEST_F(WriteOutputFileTest, SimplifiedNameSplitsOnTheLastExtensionOnly) {
  writeOutputFile(
      &blobStore_,
      pathIn("results.v2.json.gz"),
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/true);

  EXPECT_TRUE(std::filesystem::exists(pathIn("results.v2_simplified.json.gz")));
}

// A ".zstd" suffix also stays last. Otherwise the simplified name would lose
// its serialization extension and the write would fail after the run.
TEST_F(WriteOutputFileTest, SimplifiedNameKeepsTheZstdSuffixLast) {
  writeOutputFile(
      &blobStore_,
      pathIn("results.cf.zstd"),
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/true);

  EXPECT_TRUE(std::filesystem::exists(pathIn("results_simplified.cf.zstd")));
}

// FileType matches suffixes case-insensitively, so the simplified name has to
// find an upper-case compression suffix too.
TEST_F(WriteOutputFileTest, SimplifiedNameKeepsAnUppercaseSuffixLast) {
  writeOutputFile(
      &blobStore_,
      pathIn("results.JSON.GZ"),
      /*blobOutput=*/"",
      result_,
      /*addSimplifiedOutput=*/true);

  EXPECT_TRUE(std::filesystem::exists(pathIn("results_simplified.JSON.GZ")));
}

// A name is classified by FileType before any simplified name is derived from
// it, so an unsupported extension is rejected there rather than reaching the
// name-rewriting step. "gz" is the interesting case: it ends with "gz" without
// carrying a ".gz" suffix, and this is where it stops.
TEST_F(WriteOutputFileTest, UnsupportedExtensionIsRejectedBeforeAnyRewrite) {
  EXPECT_THAT(
      [this] {
        writeOutputFile(
            &blobStore_,
            /*outputFileName=*/"",
            /*blobOutput=*/"gz",
            result_,
            /*addSimplifiedOutput=*/true);
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Unsupported output file type: gz")));
}

TEST_F(WriteOutputFileTest, BlobOutputIsUploadedToTheStore) {
  constexpr auto kBlobPath = "bucket/tree/results.json";

  writeOutputFile(
      &blobStore_,
      /*outputFileName=*/"",
      kBlobPath,
      result_,
      /*addSimplifiedOutput=*/false);

  EXPECT_TRUE(blobStore_.contains(kBlobPath));
}

// Uploading the result is best effort: the simulation has already run, and its
// local output has already been written, so a store failure must not propagate.
TEST_F(WriteOutputFileTest, AStoreFailureDoesNotAbortTheWrite) {
  class ThrowingBlobStore : public IBlobStore {
   public:
    // Both overrides throw unconditionally, so they are [[noreturn]]; saying so
    // is what the store is for, and it keeps clang-tidy's missing-noreturn
    // check quiet.
    [[noreturn]] std::unique_ptr<folly::IOBuf> read(
        const std::string&) override {
      throw std::runtime_error("read not supported");
    }
    [[noreturn]] void write(
        const std::string&,
        std::unique_ptr<folly::IOBuf>,
        std::chrono::hours) override {
      throw std::runtime_error("bucket ACL missing");
    }
  } throwingStore;
  const auto path = pathIn("results.json");

  EXPECT_NO_THROW(writeOutputFile(
      &throwingStore,
      path,
      "bucket/tree/results.json",
      result_,
      /*addSimplifiedOutput=*/false));
  EXPECT_TRUE(std::filesystem::exists(path));
}

// Asking for a blob upload with no store behind it is the same situation as a
// store that fails, and gets the same treatment.
TEST_F(WriteOutputFileTest, AMissingStoreDoesNotAbortTheWrite) {
  const auto path = pathIn("results.json");

  EXPECT_NO_THROW(writeOutputFile(
      /*blobStore=*/nullptr,
      path,
      "bucket/tree/results.json",
      result_,
      /*addSimplifiedOutput=*/false));
  EXPECT_TRUE(std::filesystem::exists(path));
}

// A blob path whose type differs from the file's forces a reserialization; the
// uploaded bytes must be in the blob's format, not the file's.
TEST_F(WriteOutputFileTest, ADifferentBlobTypeIsReserialized) {
  constexpr auto kBlobPath = "bucket/tree/results.json";

  writeOutputFile(
      &blobStore_,
      pathIn("results.cf"),
      kBlobPath,
      result_,
      /*addSimplifiedOutput=*/false);

  auto uploaded = blobStore_.read(kBlobPath);
  ASSERT_NE(uploaded, nullptr) << "nothing was uploaded to " << kBlobPath;
  const auto text = uploaded->moveToFbString().toStdString();
  // The field *name* is the discriminator: compact thrift stores string values
  // as raw UTF-8, so the value alone appears in both encodings and would not
  // show the blob was reserialized out of the file's compact form.
  EXPECT_THAT(text, HasSubstr("buildRevision"));
  EXPECT_THAT(text, HasSubstr("rev-under-test"));
}

} // namespace
} // namespace packsim::io
