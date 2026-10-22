/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/file-io.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include <fmt/format.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/compression/Compression.h>
#include <folly/container/small_vector.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "packsim/util/blob-store.h"
#include "packsim/util/file-reader.h"
#include "packsim/util/simple-json-key-rewriter.h"

using apache::thrift::BinarySerializer;
using apache::thrift::CompactSerializer;
using apache::thrift::SimpleJSONSerializer;

namespace packsim::io {

namespace {
// Retention requested for blob-store output objects, in days. A store with no
// expiry, such as the local filesystem, ignores it.
constexpr auto kBlobRetentionDays = 60;
// Hours in a day; used to convert the blob retention from days to hours.
constexpr auto kHoursPerDay = 24;
// Retention period handed to IBlobStore::write.
constexpr auto kBlobRetention =
    std::chrono::hours(kHoursPerDay * kBlobRetentionDays);
// Permission bits (rw-rw-rw-) applied to newly created output files.
constexpr auto kOutputFileMode = 0666;
// Length of the ".gz" filename suffix.
constexpr size_t kGzSuffixLength = 3;
// Length of the ".zstd" filename suffix.
constexpr size_t kZstdSuffixLength = 5;

// Takes a folly::IOBuf and writes it using scatter/gather IO
void writeBufToFile(const std::string& filename, const folly::IOBuf* buf) {
  const int fd =
      open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, kOutputFileMode);
  if (fd == -1) {
    // Saved before fmt::format, which may change errno.
    const int openErrno = errno;
    throw std::system_error(
        openErrno,
        std::system_category(),
        fmt::format("Failed to open output file {}", filename));
  }
  auto iov = buf->getIov();
  const ssize_t bytesWritten =
      folly::writevFull(fd, iov.data(), static_cast<int>(iov.size()));
  if (bytesWritten == -1) {
    // Saved before close() and fmt::format, which may change errno.
    const int writeErrno = errno;
    close(fd);
    throw std::system_error(
        writeErrno,
        std::system_category(),
        fmt::format("Failed to write to output file {}", filename));
  }
  close(fd);
}

// Inserts "_simplified" before the serialization extension. The compression
// suffix FileType recognises stays last, in its original case, so the derived
// name has the same file type and directory as `filename`, and checking
// `filename` before a run covers it too.
std::string convertSimplifiedFileName(const std::string& filename) {
  if (filename.empty()) {
    return "";
  }
  std::string lowered = filename;
  folly::toLowerAscii(lowered);
  size_t compressionSuffixLength = 0;
  if (lowered.ends_with(".zstd")) {
    compressionSuffixLength = kZstdSuffixLength;
  } else if (lowered.ends_with(".gz")) {
    compressionSuffixLength = kGzSuffixLength;
  }
  const std::string stem =
      filename.substr(0, filename.size() - compressionSuffixLength);
  const size_t lastDotPos = stem.find_last_of('.');
  if (lastDotPos == std::string::npos) {
    throw std::runtime_error(
        fmt::format("No extension found in filename {}", filename));
  }
  return fmt::format(
      "{}_simplified{}{}",
      stem.substr(0, lastDotPos),
      stem.substr(lastDotPos),
      filename.substr(stem.size()));
}

/**
 * Compress a serialized buffer as a file type asks. Takes ownership so the
 * no-compression case can hand the same buffer straight back.
 *
 * @param buf   The serialized bytes; null in, null out.
 * @param type  The file type whose compression to apply.
 *
 * @return      `buf` compressed, or `buf` itself for `CompressionType::NONE`.
 *
 * @throw std::invalid_argument for a compression type outside the enum.
 */
std::unique_ptr<folly::IOBuf> maybeCompress(
    std::unique_ptr<folly::IOBuf> buf,
    FileType type) {
  if (buf == nullptr) {
    return nullptr;
  }

  switch (type.compressionType) {
    case CompressionType::NONE:
      return buf;
    case CompressionType::GZIP:
      return folly::compression::getCodec(folly::compression::CodecType::GZIP)
          ->compress(buf.get());
    case CompressionType::ZSTD:
      return folly::compression::getCodec(folly::compression::CodecType::ZSTD)
          ->compress(buf.get());
  }
  // Also reached by a value outside the enum.
  throw std::invalid_argument("Unsupported compression type");
}

/**
 * Serialize a config or result without compressing it.
 *
 * @param res                The thrift struct to serialize.
 * @param serializationType  The protocol to write it in.
 *
 * @return                   The serialized bytes, or null for
 *                           `SerializationType::NONE`.
 *
 * @throw std::invalid_argument for a serialization type outside the enum.
 */
template <typename T>
std::unique_ptr<folly::IOBuf> serializeUncompressed(
    const T& res,
    const SerializationType serializationType) {
  folly::IOBufQueue q;
  switch (serializationType) {
    case SerializationType::JSON:
      // Simple JSON writes a map keyed by structs or lists, such as
      // `nicQpnPairMap`, with objects or arrays as keys, which no JSON parser
      // accepts, so a result has those maps rewritten as lists of
      // {"key", "value"} objects. A config stays Simple JSON because
      // `deserializeConfig` reads it back with that protocol.
      if constexpr (std::is_same_v<T, packsim::thrift::SimulationResult>) {
        return folly::IOBuf::fromString(
            SimpleJsonKeyRewriter::toStandardJson(
                SimpleJSONSerializer::serialize<std::string>(res)));
      } else {
        SimpleJSONSerializer::serialize(res, &q);
        return q.move();
      }
    case SerializationType::COMPACT:
      CompactSerializer::serialize(res, &q);
      return q.move();
    case SerializationType::BINARY:
      BinarySerializer::serialize(res, &q);
      return q.move();
    case SerializationType::NONE:
      return nullptr;
  }
  // Also reached by a value outside the enum.
  throw std::invalid_argument("Unsupported output type");
}

template <typename T>
std::unique_ptr<folly::IOBuf> serialize(const T& res, FileType outputFileType) {
  return maybeCompress(
      serializeUncompressed(res, outputFileType.serializationType),
      outputFileType);
}

packsim::thrift::SimulationResult simplifyResult(
    const packsim::thrift::SimulationResult& result) {
  packsim::thrift::SimulationResult simplifiedResult;
  simplifiedResult.config()->jobInfo() = *result.config()->jobInfo();
  for (const auto& [workloadName, workloadRes] : *result.workloads()) {
    packsim::thrift::WorkloadResult simplifiedWkldRes;
    simplifiedWkldRes.name() = workloadName;
    simplifiedWkldRes.startTimeNs() = *workloadRes.startTimeNs();
    simplifiedWkldRes.endTimeNs() = *workloadRes.endTimeNs();
    simplifiedWkldRes.runTimeNs() = *workloadRes.runTimeNs();
    simplifiedWkldRes.workloadId() = *workloadRes.workloadId();
    simplifiedResult.workloads()->emplace(workloadName, simplifiedWkldRes);
  }
  return simplifiedResult;
}

packsim::thrift::Simulation readConfigFromBlobStore(
    IBlobStore& blobStore,
    const std::string& path) {
  const FileType ft(path);
  auto buf = blobStore.read(path);
  return deserializeConfig(buf.get(), ft);
}

} // namespace

// Write the output to the given file and blob path using the correct output
// format and compression type.
void writeOutputFile(
    IBlobStore* const FOLLY_NULLABLE blobStore,
    const std::string& outputFileName,
    const std::string& blobOutput,
    const packsim::thrift::SimulationResult& result,
    const bool addSimplifiedOutput,
    const bool printTiming) {
  if (outputFileName.empty() && blobOutput.empty()) {
    return;
  }
  const auto outputStart = std::chrono::system_clock::now();

  const auto outputFileType = FileType(outputFileName);
  const auto blobOutputType = FileType(blobOutput);

  // `serialize` already applies the compression the file type asks for.
  auto buf = serialize(result, outputFileType);

  if (buf != nullptr) {
    packsim::io::writeBufToFile(outputFileName, buf.get());
  }

  if (blobOutputType.serializationType != SerializationType::NONE) {
    // Only reserialize when the blob wants a different serialization or
    // compression; otherwise the buffer just written locally is reused.
    if (blobOutputType != outputFileType) {
      buf = serialize(result, blobOutputType);
    }

    // Result upload is best-effort: a store failure (e.g. a missing bucket
    // ACL) must not abort an otherwise-successful simulation whose local output
    // has already been written. A caller that supplied no store at all is the
    // same situation and gets the same treatment.
    try {
      if (blobStore == nullptr) {
        throw std::runtime_error("no blob store was provided");
      }
      blobStore->write(blobOutput, std::move(buf), kBlobRetention);
    } catch (const std::exception& e) {
      XLOGF(
          ERR,
          "Failed to write simulation result to blob path {}: {}",
          blobOutput,
          e.what());
    }
  }

  if (addSimplifiedOutput) {
    writeOutputFile(
        blobStore,
        convertSimplifiedFileName(outputFileName),
        convertSimplifiedFileName(blobOutput),
        simplifyResult(result),
        /*addSimplifiedOutput=*/false, // Don't recurse again
        /*printTiming=*/false); // Don't output timing in sub-call
  }

  if (printTiming) {
    const auto outputEnd = std::chrono::system_clock::now();
    const std::chrono::duration<double> outputElapsedSeconds =
        outputEnd - outputStart;
    XLOGF(
        INFO,
        "Output serialization running time (includes blob store): {} seconds",
        outputElapsedSeconds.count());
  }
}

void writeConfigToBlobStore(
    IBlobStore& blobStore,
    const std::string& path,
    packsim::thrift::Simulation const& simCfg) {
  const auto blobOutputType = FileType(path);

  if (blobOutputType.serializationType == SerializationType::NONE) {
    XLOG(WARN, "Serialization type was not set.");
    return;
  }

  // `serialize` already applies the compression the path's file type asks for.
  blobStore.write(path, serialize(simCfg, blobOutputType), kBlobRetention);
}

bool readConfig(
    IBlobStore* const FOLLY_NULLABLE blobStore,
    const std::string& filePath,
    const std::string& blobPath,
    packsim::thrift::Simulation& cfg) {
  if (filePath.size() && blobPath.size()) {
    throw std::runtime_error("Cannot specify both file and blob path");
  }
  if (filePath.size()) {
    cfg = readConfigFromFile(filePath);
    return true;
  } else if (blobPath.size()) {
    if (blobStore == nullptr) {
      throw std::runtime_error(
          "A blob path was given but no blob store is available");
    }
    cfg = readConfigFromBlobStore(*blobStore, blobPath);
    return true;
  }
  return false;
}

bool checkOutputPathAndCreateDir(
    const std::string& outputFileName,
    const bool overwrite) {
  if (!outputFileName.size()) {
    return true;
  }
  // symlink_status does not follow symlinks, so a dangling symlink still counts
  // as "already exists" and, like any existing entry, is replaced only when
  // `overwrite` is set.
  std::error_code ec;
  if (std::filesystem::exists(
          std::filesystem::symlink_status(outputFileName, ec))) {
    if (!overwrite) {
      XLOGF(ERR, "Output file {} already exists", outputFileName);
      return false;
    }
    XLOGF(
        INFO,
        "Output file {} already exists and will be overwritten",
        outputFileName);
  }
  // An output name with no directory part leaves parent_path() empty, which
  // means "write into the cwd" and needs no mkdir.
  const auto dirname = std::filesystem::path(outputFileName).parent_path();
  if (!dirname.empty() && !std::filesystem::is_directory(dirname, ec)) {
    std::filesystem::create_directories(dirname, ec);
    if (ec) {
      XLOGF(
          ERR,
          "Couldn't create directory {}: {}",
          dirname.string(),
          ec.message());
      return false;
    }
  }
  return true;
}

void prepareOutputDestinations(
    const std::string& outputFileName,
    const std::string& blobOutput) {
  // Constructed for the std::invalid_argument an unsupported extension throws,
  // before anything below touches the filesystem.
  [[maybe_unused]] const FileType outputFileType{outputFileName};
  [[maybe_unused]] const FileType blobOutputType{blobOutput};
  // The shell leaves "~" in "--output_file=~/..." unexpanded, and creating the
  // parent directories would then make a directory literally named "~".
  if (outputFileName.starts_with('~')) {
    throw std::invalid_argument(
        fmt::format(
            "Can't expand ~ in output file {}; spell out the home directory",
            outputFileName));
  }
  // Follows symlinks, as the open() in writeBufToFile does: a symlink to a
  // directory fails there with EISDIR too.
  std::error_code ec;
  if (std::filesystem::is_directory(outputFileName, ec)) {
    throw std::invalid_argument(
        fmt::format("Output file {} is a directory", outputFileName));
  }
  if (!checkOutputPathAndCreateDir(outputFileName, /*overwrite=*/true)) {
    throw std::runtime_error(
        fmt::format(
            "Cannot create the directory for output file {}", outputFileName));
  }
}

} // namespace packsim::io
