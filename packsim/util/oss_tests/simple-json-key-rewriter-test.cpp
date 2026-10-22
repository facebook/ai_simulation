/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/simple-json-key-rewriter.h"

namespace packsim::io {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

TEST(SimpleJsonKeyRewriterTest, RewritesAStructKeyedMapAsAKeyValueList) {
  EXPECT_EQ(
      SimpleJsonKeyRewriter::toStandardJson(
          R"({"m":{{"a":1,"b":2}:[3],{"a":4,"b":5}:[]}})"),
      R"({"m":[{"key":{"a":1,"b":2},"value":[3]},{"key":{"a":4,"b":5},"value":[]}]})");
}

TEST(SimpleJsonKeyRewriterTest, RewritesAListKeyedMapAsAKeyValueList) {
  EXPECT_EQ(
      SimpleJsonKeyRewriter::toStandardJson(R"({"m":{[1,2]:"x",[]:"y"}})"),
      R"({"m":[{"key":[1,2],"value":"x"},{"key":[],"value":"y"}]})");
}

TEST(SimpleJsonKeyRewriterTest, RewritesMapsNestedInKeysAndValues) {
  EXPECT_EQ(
      SimpleJsonKeyRewriter::toStandardJson(
          R"({"m":{{"k":{[1]:2}}:{"v":{{"a":1}:3}}}})"),
      R"({"m":[{"key":{"k":[{"key":[1],"value":2}]},"value":{"v":[{"key":{"a":1},"value":3}]}}]})");
}

TEST(SimpleJsonKeyRewriterTest, CopiesStringKeyedObjectsAndScalarsUnchanged) {
  const std::string json =
      R"({"s":{"1":2,"x":"y"},"n":-1.5e+30,"t":true,"f":false,"e":{},"a":[],"d":"NaN"})";

  EXPECT_EQ(SimpleJsonKeyRewriter::toStandardJson(json), json);
}

TEST(SimpleJsonKeyRewriterTest, CopiesStringsContainingJsonSyntaxUnchanged) {
  const std::string json = R"({"s":"{[\"k\":1]},\\","t":{"\"{":1}})";

  EXPECT_EQ(SimpleJsonKeyRewriter::toStandardJson(json), json);
}

TEST(SimpleJsonKeyRewriterTest, ThrowsOnTruncatedInput) {
  EXPECT_THAT(
      [] { SimpleJsonKeyRewriter::toStandardJson(R"({"m":{{"a":1}:)"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Not Simple JSON at byte 14: unexpected end of input")));
}

TEST(SimpleJsonKeyRewriterTest, ThrowsOnAMissingSeparator) {
  EXPECT_THAT(
      [] { SimpleJsonKeyRewriter::toStandardJson(R"({"a":1])"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Not Simple JSON at byte 6: expected ',' or '}'")));
}

TEST(SimpleJsonKeyRewriterTest, ThrowsOnAMissingValue) {
  EXPECT_THAT(
      [] { SimpleJsonKeyRewriter::toStandardJson(R"({"a":})"); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Not Simple JSON at byte 5: expected a value")));
}

TEST(SimpleJsonKeyRewriterTest, ThrowsOnTrailingContent) {
  EXPECT_THAT(
      [] { SimpleJsonKeyRewriter::toStandardJson("{}{}"); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Not Simple JSON at byte 2: expected the end of the input")));
}

} // namespace packsim::io
