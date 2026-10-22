/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/simple-json-key-rewriter.h"

#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>

#include <fmt/format.h>

namespace packsim::io {

namespace {

/**
 * Rewriter copies one Simple JSON value into standard JSON in a single pass.
 *
 * Simple JSON has no whitespace, so the byte after `{` tells a string-keyed
 * object (`"`) from a map with object or array keys (`{` or `[`); a map's keys
 * all share one type, so its first key decides the form of the whole map.
 */
class Rewriter {
 public:
  explicit Rewriter(const std::string& input) : input_(input) {
    // Each rewritten map entry grows by 16 bytes. The headroom keeps a typical
    // result from reallocating, and so copying, the whole output.
    output_.reserve(input.size() + input.size() / 4);
  }

  /**
   * Rewrite the whole input.
   *
   * @return          The standard JSON.
   *
   * @throw std::invalid_argument if the input is not one Simple JSON value.
   */
  std::string rewrite() && {
    copyValue();
    if (pos_ != input_.size()) {
      fail("expected the end of the input");
    }
    return std::move(output_);
  }

 private:
  void copyValue() {
    switch (peek()) {
      case '{':
        copyObject();
        return;
      case '[':
        copyArray();
        return;
      case '"':
        copyString();
        return;
      default:
        copyScalar();
        return;
    }
  }

  void copyObject() {
    expect('{');
    if (peek() == '}') {
      ++pos_;
      output_ += "{}";
    } else if (peek() == '"') {
      copyMembers();
    } else {
      copyEntriesAsList();
    }
  }

  /**
   * Copy `{"k":v,...}` unchanged, starting after its opening brace.
   */
  void copyMembers() {
    output_.push_back('{');
    do {
      copyString();
      expect(':');
      output_.push_back(':');
      copyValue();
    } while (copyCommaOr('}'));
    output_.push_back('}');
  }

  /**
   * Write `{K:V,...}` as `[{"key":K,"value":V},...]`, starting after its
   * opening brace.
   */
  void copyEntriesAsList() {
    output_.push_back('[');
    do {
      output_ += R"({"key":)";
      copyValue();
      expect(':');
      output_ += R"(,"value":)";
      copyValue();
      output_.push_back('}');
    } while (copyCommaOr('}'));
    output_.push_back(']');
  }

  void copyArray() {
    expect('[');
    output_.push_back('[');
    if (peek() == ']') {
      ++pos_;
    } else {
      do {
        copyValue();
      } while (copyCommaOr(']'));
    }
    output_.push_back(']');
  }

  /**
   * Copy the separator after a member, element or entry.
   *
   * @param close     The bracket that closes the container, `}` or `]`.
   *
   * @return          True after a `,`, false after `close`.
   *
   * @throw std::invalid_argument if the next byte is neither.
   */
  bool copyCommaOr(char close) {
    const char c = next();
    if (c == ',') {
      output_.push_back(',');
      return true;
    }
    if (c != close) {
      --pos_;
      fail(fmt::format("expected ',' or '{}'", close));
    }
    return false;
  }

  void copyString() {
    const size_t start = pos_;
    expect('"');
    for (char c = next(); c != '"'; c = next()) {
      if (c == '\\') {
        next();
      }
    }
    output_.append(input_, start, pos_ - start);
  }

  /**
   * Copy a number, `true`, `false` or `null`, up to the next delimiter.
   */
  void copyScalar() {
    const size_t start = pos_;
    while (pos_ < input_.size() && !isDelimiter(input_[pos_])) {
      ++pos_;
    }
    if (pos_ == start) {
      fail("expected a value");
    }
    output_.append(input_, start, pos_ - start);
  }

  static bool isDelimiter(char c) noexcept {
    return c == ',' || c == ':' || c == '}' || c == ']';
  }

  char peek() const {
    if (pos_ >= input_.size()) {
      fail("unexpected end of input");
    }
    return input_[pos_];
  }

  char next() {
    const char c = peek();
    ++pos_;
    return c;
  }

  void expect(char expected) {
    if (peek() != expected) {
      fail(fmt::format("expected '{}'", expected));
    }
    ++pos_;
  }

  [[noreturn]] void fail(const std::string& reason) const {
    throw std::invalid_argument(
        fmt::format("Not Simple JSON at byte {}: {}", pos_, reason));
  }

  // The Simple JSON being rewritten; outlives the rewriter.
  const std::string& input_;
  // The index of the next byte of `input_` to read.
  size_t pos_ = 0;
  // The standard JSON written so far.
  std::string output_;
};

} // namespace

std::string SimpleJsonKeyRewriter::toStandardJson(
    const std::string& simpleJson) {
  return Rewriter(simpleJson).rewrite();
}

} // namespace packsim::io
