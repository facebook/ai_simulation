/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/file-reader.h"

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "packsim/util/simple-json-field-checker.h"

using apache::thrift::BinarySerializer;
using apache::thrift::CompactSerializer;
using apache::thrift::SimpleJSONSerializer;

namespace {
// Length of the ".zstd" filename suffix.
constexpr int kZstdSuffixLength = 5;
// Length of the ".gz" filename suffix.
constexpr int kGzSuffixLength = 3;
// Most unknown config fields one warning names. A config written against
// another version of the schema can repeat a field in every workload.
constexpr size_t kMaxLoggedUnknownFields = 20;
// Largest JSON config checked for fields the schema does not define, 8 MiB.
// The check holds two parsed copies of the config, which can take 40 times its
// size.
constexpr size_t kMaxCheckedJsonConfigBytes = size_t{8} << 20;

// Logs the keys of the Simple JSON config in `buf` that reading it into `cfg`
// dropped.
void logUnknownFields(
    const folly::IOBuf& buf,
    const packsim::thrift::Simulation& cfg) {
  const size_t configBytes = buf.computeChainDataLength();
  if (configBytes > kMaxCheckedJsonConfigBytes) {
    XLOGF(
        INFO,
        "Config not checked for fields this build's schema does not define: "
        "at {} bytes it is over the check's limit of {} bytes",
        configBytes,
        kMaxCheckedJsonConfigBytes);
    return;
  }
  auto unknown = packsim::io::SimpleJsonFieldChecker::findUnknownFields(
      buf.toString(), cfg);
  if (!unknown.has_value()) {
    XLOGF(
        INFO,
        "Config not checked for fields this build's schema does not define: "
        "only standard JSON, or Simple JSON without whitespace, can be checked");
    return;
  }
  if (unknown->empty()) {
    return;
  }
  const size_t total = unknown->size();
  unknown->resize(std::min(total, kMaxLoggedUnknownFields));
  XLOGF(
      WARN,
      "Ignored config fields this build's schema does not define: {}{}",
      fmt::join(*unknown, ", "),
      total > unknown->size()
          ? fmt::format(", and {} more", total - unknown->size())
          : std::string());
}

// Reads a Simple JSON config and logs the keys reading it dropped.
packsim::thrift::Simulation deserializeJsonConfig(const folly::IOBuf& buf) {
  auto cfg =
      SimpleJSONSerializer::deserialize<packsim::thrift::Simulation>(&buf);
  // The check is diagnostic: a failure in it must not fail a config that
  // deserialized.
  try {
    logUnknownFields(buf, cfg);
  } catch (const std::exception& e) {
    XLOGF(
        INFO,
        "Config not checked for fields this build's schema does not define: {}",
        e.what());
  }
  return cfg;
}
} // namespace

namespace packsim::io {
// The output serializer to use

// Parse the output filename and return the output type and compression type
FileType::FileType(const std::string& filename) {
  if (filename.empty()) {
    return;
  }
  // A file is opened through `c_str()`, which stops at the first NUL, so the
  // suffixes read below would not be those of the name that is opened.
  if (filename.find('\0') != std::string::npos) {
    throw std::invalid_argument(
        fmt::format(
            "File name '{}' must not contain a NUL byte",
            folly::cEscape<std::string>(filename)));
  }

  // Suffixes are matched case-insensitively, so ".JSON.GZ" classifies the
  // same as ".json.gz".
  std::string lowered = filename;
  folly::toLowerAscii(lowered);
  std::string_view file(lowered);
  if (file.ends_with(".zstd")) {
    compressionType = CompressionType::ZSTD;
    file.remove_suffix(kZstdSuffixLength); // Remove ".zstd" suffix
  } else if (file.ends_with(".gz")) {
    compressionType = CompressionType::GZIP;
    file.remove_suffix(kGzSuffixLength); // Remove ".gz" suffix
  }

  if (file.ends_with(".json")) {
    serializationType = SerializationType::JSON;
  } else if (file.ends_with(".cf")) {
    serializationType = SerializationType::COMPACT;
  } else if (file.ends_with(".bin")) {
    serializationType = SerializationType::BINARY;
  } else {
    throw std::invalid_argument(
        fmt::format(
            "Unsupported output file type: {}, supported types are .json, .cf and .bin",
            filename));
  }
}

packsim::thrift::Simulation deserializeConfig(
    folly::IOBuf* buf,
    const FileType& type) {
  if (buf == nullptr) {
    throw std::invalid_argument("No config data to read");
  }
  std::unique_ptr<folly::IOBuf> decompressed;
  if (type.compressionType == CompressionType::GZIP) {
    decompressed =
        folly::compression::getCodec(folly::compression::CodecType::GZIP)
            ->uncompress(buf);
  } else if (type.compressionType == CompressionType::ZSTD) {
    decompressed =
        folly::compression::getCodec(folly::compression::CodecType::ZSTD)
            ->uncompress(buf);
  }
  const folly::IOBuf& data = decompressed ? *decompressed : *buf;
  switch (type.serializationType) {
    case SerializationType::JSON:
      return deserializeJsonConfig(data);
    case SerializationType::COMPACT:
      return CompactSerializer::deserialize<packsim::thrift::Simulation>(&data);
    case SerializationType::BINARY:
      return BinarySerializer::deserialize<packsim::thrift::Simulation>(&data);
    case SerializationType::NONE:
      throw std::invalid_argument("No output type specified");
  }
  // Also reached by a value outside the enum.
  throw std::invalid_argument("Unsupported output type");
}

packsim::thrift::Simulation readConfigFromFile(const std::string& path) {
  if (path.at(0) == '~') {
    throw std::runtime_error(
        fmt::format(
            "Can't expand ~ in config path; use `--json_cfg {}` instead",
            path));
  }
  const auto ft = FileType(path);
  std::string contents;
  if (!folly::readFile(path.c_str(), contents)) {
    // exists() follows symlinks and the error_code overload cannot throw, so a
    // dangling symlink or an unreadable parent reports "does not exist" here
    // exactly as the previous FileUtil::actualFileExists did.
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
      throw std::runtime_error(fmt::format("File does not exist: {}", path));
    }
    throw std::runtime_error(fmt::format("Couldn't load config from {}", path));
  }

  const auto buf = folly::IOBuf::fromString(contents);
  return deserializeConfig(buf.get(), ft);
}

} // namespace packsim::io
