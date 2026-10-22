/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/LogCategory.h>
#include <folly/logging/LogHandler.h>
#include <folly/logging/LogHandlerConfig.h>
#include <folly/logging/LogLevel.h>
#include <folly/logging/LogMessage.h>
#include <folly/logging/LoggerDB.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/file-reader.h"

using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::ThrowsMessage;

namespace packsim::io {

namespace {

// Collects every message logged to the category it is attached to.
class LogCollector : public folly::LogHandler {
 public:
  void handleMessage(
      const folly::LogMessage& message,
      const folly::LogCategory* /* handlerCategory */) override {
    messages_.emplace_back(message.getLevel(), message.getMessage());
  }

  void flush() override {}

  folly::LogHandlerConfig getConfig() const override {
    return folly::LogHandlerConfig("collector");
  }

  /**
   * Get the messages logged at one level.
   *
   * @param level     The level to select, e.g. `folly::LogLevel::WARN`.
   *
   * @return          The text of each message logged at `level`, oldest first.
   */
  std::vector<std::string> getMessagesAt(folly::LogLevel level) const {
    std::vector<std::string> selected;
    for (const auto& [messageLevel, text] : messages_) {
      if (messageLevel == level) {
        selected.emplace_back(text);
      }
    }
    return selected;
  }

 private:
  // Level and text of each message, oldest first.
  std::vector<std::pair<folly::LogLevel, std::string>> messages_;
};

} // namespace

// Attaches a LogCollector to the root log category for the length of a test,
// so a test can see what loading a config logs.
class DeserializeConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedHandlers_ = getRootCategory().getHandlers();
    getRootCategory().addHandler(collector_);
  }

  void TearDown() override {
    getRootCategory().replaceHandlers(savedHandlers_);
  }

  // The collector attached to the root log category.
  const LogCollector& getCollector() const noexcept {
    return *collector_;
  }

 private:
  // The category every message reaches.
  static folly::LogCategory& getRootCategory() {
    auto* const root = folly::LoggerDB::get().getCategory("");
    if (root == nullptr) {
      throw std::runtime_error("The logger database has no root category");
    }
    return *root;
  }

  // Receives every message logged while the test runs.
  std::shared_ptr<LogCollector> collector_{std::make_shared<LogCollector>()};
  // Handlers the root category had before the test, restored after it.
  std::vector<std::shared_ptr<folly::LogHandler>> savedHandlers_;
};

// A missing input file must surface a clear, catchable error rather than an
// unhandled exception that aborts the process.
TEST(FileReaderTest, ReadConfigFromFileThrowsWhenFileMissing) {
  EXPECT_THAT(
      []() { readConfigFromFile("/nonexistent_dir_packsim_test/missing.cf"); },
      ThrowsMessage<std::runtime_error>(HasSubstr("File does not exist")));
}

// A "~" home-directory path is not expanded by the reader, so it must report a
// helpful error pointing at the literal path instead of failing obscurely.
TEST(FileReaderTest, ReadConfigFromFileRejectsTildePath) {
  EXPECT_THAT(
      []() { readConfigFromFile("~/missing.cf"); },
      ThrowsMessage<std::runtime_error>(HasSubstr("Can't expand ~")));
}

TEST(FileReaderTest, DeserializeConfigRejectsANullBuffer) {
  EXPECT_THAT(
      []() { deserializeConfig(nullptr, FileType("cfg.json")); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("No config data")));
}

// ── FileType ─────────────────────────────────────────────────────────────────
//
// FileType is how packsim decides, from the filename alone, how to
// serialize/deserialize a config and whether it is compressed. Getting it
// wrong silently produces an unreadable file, so each suffix combination is
// pinned individually rather than by a loop over a table.

TEST(FileTypeTest, EmptyFilenameSelectsNoSerializationAndNoCompression) {
  const FileType type("");

  EXPECT_EQ(SerializationType::NONE, type.serializationType);
  EXPECT_EQ(CompressionType::NONE, type.compressionType);
}

TEST(FileTypeTest, JsonSuffixSelectsJsonUncompressed) {
  const FileType type("sim.json");

  EXPECT_EQ(SerializationType::JSON, type.serializationType);
  EXPECT_EQ(CompressionType::NONE, type.compressionType);
}

TEST(FileTypeTest, CfSuffixSelectsCompactUncompressed) {
  const FileType type("sim.cf");

  EXPECT_EQ(SerializationType::COMPACT, type.serializationType);
  EXPECT_EQ(CompressionType::NONE, type.compressionType);
}

TEST(FileTypeTest, BinSuffixSelectsBinaryUncompressed) {
  const FileType type("sim.bin");

  EXPECT_EQ(SerializationType::BINARY, type.serializationType);
  EXPECT_EQ(CompressionType::NONE, type.compressionType);
}

// The compression suffix is stripped first, so the serialization suffix is
// read from what is left. Both halves have to be picked up from one name.
TEST(FileTypeTest, GzSuffixIsStrippedBeforeTheSerializationSuffixIsRead) {
  const FileType type("sim.json.gz");

  EXPECT_EQ(SerializationType::JSON, type.serializationType);
  EXPECT_EQ(CompressionType::GZIP, type.compressionType);
}

TEST(FileTypeTest, ZstdSuffixIsStrippedBeforeTheSerializationSuffixIsRead) {
  const FileType type("sim.cf.zstd");

  EXPECT_EQ(SerializationType::COMPACT, type.serializationType);
  EXPECT_EQ(CompressionType::ZSTD, type.compressionType);
}

TEST(FileTypeTest, BinaryCanAlsoBeCompressed) {
  const FileType type("sim.bin.zstd");

  EXPECT_EQ(SerializationType::BINARY, type.serializationType);
  EXPECT_EQ(CompressionType::ZSTD, type.compressionType);
}

// Classification is a chain of ends_with checks in FileType's constructor, so
// dots in directory components and in the basename are simply not consulted.
// This pins that: it is a regression guard against a rewrite that reaches for
// the extension by scanning for a dot, not extra branch coverage over the
// plain .json.gz case above.
TEST(FileTypeTest, ClassificationUsesTheTrailingSuffixesOfAFullPath) {
  const FileType type("/tmp/run.2026.01/my.sim.json.gz");

  EXPECT_EQ(SerializationType::JSON, type.serializationType);
  EXPECT_EQ(CompressionType::GZIP, type.compressionType);
}

// Suffix matching folds case, so a path written with upper-case extensions is
// classified rather than rejected as an unsupported file type.
TEST(FileTypeTest, ClassificationIgnoresSuffixCase) {
  const FileType type("sim.JSON.GZ");

  EXPECT_EQ(SerializationType::JSON, type.serializationType);
  EXPECT_EQ(CompressionType::GZIP, type.compressionType);
}

// The message also names the offending filename, so the operator can see which
// output path they got wrong. Asserted here rather than in a second throwing
// case: the branch is the same one, only the substring differs.
TEST(FileTypeTest, UnknownSerializationSuffixThrowsNamingTheFile) {
  EXPECT_THAT(
      []() { FileType("sim.txt"); },
      ThrowsMessage<std::invalid_argument>(AllOf(
          HasSubstr("Unsupported output file type"), HasSubstr("sim.txt"))));
}

// A bare compression suffix leaves nothing recognisable behind once stripped,
// so it is rejected rather than silently treated as uncompressed.
TEST(FileTypeTest, CompressionSuffixWithoutASerializationSuffixThrows) {
  EXPECT_THAT(
      []() { FileType("sim.gz"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Unsupported output file type")));
}

// The file would be opened through `c_str()`, which ends the name at the NUL,
// so the `.json` the classification sees is not part of the name opened.
TEST(FileTypeTest, NulByteInTheNameThrows) {
  const std::string name = std::string("results") + '\0' + ".json";

  EXPECT_THAT(
      [&]() { FileType{name}; },
      ThrowsMessage<std::invalid_argument>(HasSubstr("NUL byte")));
}

// EXPECT_EQ/EXPECT_NE rather than EXPECT_TRUE over the operator: the boolean
// would be collapsed before gtest saw it, so a failure could not report which
// serialization/compression pair was actually produced.
TEST(FileTypeTest, TypesWithTheSameSuffixesCompareEqual) {
  EXPECT_EQ(FileType("a.json.gz"), FileType("b.json.gz"));
}

TEST(FileTypeTest, DifferingSerializationMakesTypesUnequal) {
  EXPECT_NE(FileType("a.json"), FileType("a.cf"));
}

TEST(FileTypeTest, DifferingCompressionMakesTypesUnequal) {
  EXPECT_NE(FileType("a.json"), FileType("a.json.gz"));
}

// ── Unknown JSON config fields ───────────────────────────────────────────────
//
// An unknown key is logged, not rejected: the rest of the config still loads.

TEST_F(DeserializeConfigTest, JsonConfigWithUnknownFieldsLoadsAndWarns) {
  const auto buf =
      folly::IOBuf::copyBuffer(R"({"name":"kept","notASimulationField":1})");

  const auto cfg = deserializeConfig(buf.get(), FileType("cfg.json"));

  EXPECT_EQ(*cfg.name(), "kept");
  EXPECT_THAT(
      getCollector().getMessagesAt(folly::LogLevel::WARN),
      ElementsAre(HasSubstr("notASimulationField")));
}

TEST_F(DeserializeConfigTest, JsonConfigWithOnlyKnownFieldsDoesNotWarn) {
  const auto buf = folly::IOBuf::copyBuffer(R"({"name":"kept"})");

  deserializeConfig(buf.get(), FileType("cfg.json"));

  EXPECT_THAT(getCollector().getMessagesAt(folly::LogLevel::WARN), IsEmpty());
}

// A config written against another version of the schema can carry the same
// unknown field many times over, so the warning names only the first 20 paths
// and counts the rest.
TEST_F(DeserializeConfigTest, WarningNamesTwentyUnknownFieldsAndCountsTheRest) {
  std::string json = R"({"name":"kept")";
  for (int i = 0; i < 22; ++i) {
    json += fmt::format(R"(,"unknownField{:02}":1)", i);
  }
  json += "}";
  const auto buf = folly::IOBuf::copyBuffer(json);

  deserializeConfig(buf.get(), FileType("cfg.json"));

  EXPECT_THAT(
      getCollector().getMessagesAt(folly::LogLevel::WARN),
      ElementsAre(AllOf(
          HasSubstr("unknownField00, "),
          HasSubstr("unknownField19, and 2 more"),
          Not(HasSubstr("unknownField20")))));
}

// Checking holds two parsed copies of a config, so a config over 8 MiB is
// loaded unchecked and the log says so.
TEST_F(DeserializeConfigTest, JsonConfigOverTheSizeLimitIsLoggedAsUnchecked) {
  const std::string name(std::size_t{8} << 20, 'x');
  const auto buf = folly::IOBuf::copyBuffer(
      fmt::format(R"({{"name":"{}","notASimulationField":1}})", name));

  const auto cfg = deserializeConfig(buf.get(), FileType("cfg.json"));

  EXPECT_EQ(*cfg.name(), name);
  EXPECT_THAT(getCollector().getMessagesAt(folly::LogLevel::WARN), IsEmpty());
  EXPECT_THAT(
      getCollector().getMessagesAt(folly::LogLevel::INFO),
      ElementsAre(HasSubstr("over the check's limit")));
}

// With whitespace, a map keyed by structs leaves Simple JSON that neither
// parses as JSON nor rewrites into it, so the config loads unchecked and the
// log says so.
TEST_F(
    DeserializeConfigTest,
    JsonConfigThatCannotBeCheckedIsLoggedAsUnchecked) {
  const auto buf = folly::IOBuf::copyBuffer(R"({
    "newWorkloadSchedule": {"workloads": [{
      "name": "w",
      "nicQpnPairMap": {
        {"srcRank": 0, "dstRank": 1}: {{"srcNicIdx": 0, "dstNicIdx": 0}: []}
      }
    }]}
  })");

  const auto cfg = deserializeConfig(buf.get(), FileType("cfg.json"));

  EXPECT_EQ(cfg.newWorkloadSchedule()->workloads()->size(), 1U);
  EXPECT_THAT(
      getCollector().getMessagesAt(folly::LogLevel::INFO),
      ElementsAre(HasSubstr("Config not checked")));
}

} // namespace packsim::io
