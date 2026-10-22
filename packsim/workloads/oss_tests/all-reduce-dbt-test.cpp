/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/workloads/all-reduce-dbt.h"

namespace packsim {
namespace {

// Checks the structural invariants a double binary tree over `n` ranks must
// satisfy, independent of orientation: sizes, a single root with no parent, the
// expected branching factor, consistent parent<->child back-references, every
// non-root node referenced as a child exactly once, reachability of all nodes
// from the root, and a leaf set equal to the set of childless nodes.
void expectValidDoubleBinaryTree(const BinaryMessageTree& tree, uint32_t n) {
  ASSERT_EQ(tree.parent.size(), n);
  ASSERT_EQ(tree.children.size(), n);
  ASSERT_LT(tree.root, n);
  EXPECT_EQ(tree.parent.at(tree.root), UINT32_MAX);

  std::vector<uint32_t> childRefCount(n, 0);
  for (uint32_t parent = 0; parent < n; ++parent) {
    const auto& children = tree.children.at(parent);
    // The root fans out to a single child; interior nodes are binary; leaves
    // have none.
    if (parent == tree.root) {
      EXPECT_EQ(children.size(), 1u);
    } else if (!children.empty()) {
      EXPECT_EQ(children.size(), 2u);
    }
    for (uint32_t child : children) {
      ASSERT_LT(child, n);
      EXPECT_EQ(tree.parent.at(child), parent);
      childRefCount.at(child)++;
    }
  }
  for (uint32_t node = 0; node < n; ++node) {
    // The root is nobody's child; every other node has exactly one parent.
    EXPECT_EQ(childRefCount.at(node), node == tree.root ? 0u : 1u);
  }

  // In-degree constraints alone permit a detached cycle, so walk from the root
  // and require every node to be reachable.
  std::set<uint32_t> reachable;
  std::vector<uint32_t> stack{tree.root};
  while (!stack.empty()) {
    const uint32_t node = stack.back();
    stack.pop_back();
    if (!reachable.insert(node).second) {
      continue;
    }
    for (uint32_t child : tree.children.at(node)) {
      stack.emplace_back(child);
    }
  }
  EXPECT_EQ(reachable.size(), n);

  std::set<uint32_t> leafSet;
  for (uint32_t leaf : tree.leaves) {
    ASSERT_LT(leaf, n);
    leafSet.insert(leaf);
  }
  EXPECT_EQ(leafSet.size(), tree.leaves.size()); // leaves are distinct
  for (uint32_t node = 0; node < n; ++node) {
    EXPECT_EQ(leafSet.contains(node), tree.children.at(node).empty());
  }
}

TEST(BuildDoubleBinaryTreeTest, OddLeavesTreeForEightRanks) {
  const auto& tree = buildDoubleBinaryTree(8, /*oddLeaves=*/true);
  EXPECT_EQ(tree.root, 0u);
  EXPECT_EQ(tree.children.at(0), (std::vector<uint32_t>{4}));
  EXPECT_EQ(tree.children.at(4), (std::vector<uint32_t>{2, 6}));
  EXPECT_EQ(tree.children.at(2), (std::vector<uint32_t>{1, 3}));
  EXPECT_EQ(tree.children.at(6), (std::vector<uint32_t>{5, 7}));
  EXPECT_EQ(tree.leaves, (std::vector<uint32_t>{1, 3, 5, 7}));
  EXPECT_EQ(tree.parent.at(4), 0u);
  EXPECT_EQ(tree.parent.at(2), 4u);
}

TEST(BuildDoubleBinaryTreeTest, EvenLeavesTreeForEightRanks) {
  const auto& tree = buildDoubleBinaryTree(8, /*oddLeaves=*/false);
  EXPECT_EQ(tree.root, 7u);
  EXPECT_EQ(tree.children.at(7), (std::vector<uint32_t>{3}));
  EXPECT_EQ(tree.children.at(3), (std::vector<uint32_t>{1, 5}));
  EXPECT_EQ(tree.children.at(1), (std::vector<uint32_t>{0, 2}));
  EXPECT_EQ(tree.children.at(5), (std::vector<uint32_t>{4, 6}));
  EXPECT_EQ(tree.leaves, (std::vector<uint32_t>{0, 2, 4, 6}));
  EXPECT_EQ(tree.parent.at(7), UINT32_MAX);
}

TEST(BuildDoubleBinaryTreeTest, OddLeavesTreeForFourRanks) {
  const auto& tree = buildDoubleBinaryTree(4, /*oddLeaves=*/true);
  EXPECT_EQ(tree.root, 0u);
  EXPECT_EQ(tree.children.at(0), (std::vector<uint32_t>{2}));
  EXPECT_EQ(tree.children.at(2), (std::vector<uint32_t>{1, 3}));
  EXPECT_EQ(tree.leaves, (std::vector<uint32_t>{1, 3}));
}

TEST(BuildDoubleBinaryTreeTest, EvenLeavesTreeForFourRanks) {
  const auto& tree = buildDoubleBinaryTree(4, /*oddLeaves=*/false);
  EXPECT_EQ(tree.root, 3u);
  EXPECT_EQ(tree.children.at(3), (std::vector<uint32_t>{1}));
  EXPECT_EQ(tree.children.at(1), (std::vector<uint32_t>{0, 2}));
  EXPECT_EQ(tree.leaves, (std::vector<uint32_t>{0, 2}));
}

TEST(BuildDoubleBinaryTreeTest, TwoRankOddLeavesTreeIsRootAndSingleLeaf) {
  const auto& tree = buildDoubleBinaryTree(2, /*oddLeaves=*/true);
  EXPECT_EQ(tree.root, 0u);
  EXPECT_EQ(tree.children.at(0), (std::vector<uint32_t>{1}));
  EXPECT_EQ(tree.leaves, (std::vector<uint32_t>{1}));
}

TEST(BuildDoubleBinaryTreeTest, TwoRankEvenLeavesTreeIsRootAndSingleLeaf) {
  const auto& tree = buildDoubleBinaryTree(2, /*oddLeaves=*/false);
  EXPECT_EQ(tree.root, 1u);
  EXPECT_EQ(tree.children.at(1), (std::vector<uint32_t>{0}));
  EXPECT_EQ(tree.leaves, (std::vector<uint32_t>{0}));
}

TEST(BuildDoubleBinaryTreeTest, OddLeavesStructureIsValidAcrossSizes) {
  for (uint32_t n : {2u, 4u, 8u, 16u}) {
    SCOPED_TRACE(n);
    expectValidDoubleBinaryTree(
        buildDoubleBinaryTree(n, /*oddLeaves=*/true), n);
  }
}

TEST(BuildDoubleBinaryTreeTest, EvenLeavesStructureIsValidAcrossSizes) {
  for (uint32_t n : {2u, 4u, 8u, 16u}) {
    SCOPED_TRACE(n);
    expectValidDoubleBinaryTree(
        buildDoubleBinaryTree(n, /*oddLeaves=*/false), n);
  }
}

TEST(BuildDoubleBinaryTreeTest, OddAndEvenLeavesAreMirroredRoots) {
  // The two orientations root at opposite ends so their union spans both
  // reduction directions.
  const auto& oddTree = buildDoubleBinaryTree(8, /*oddLeaves=*/true);
  const auto& evenTree = buildDoubleBinaryTree(8, /*oddLeaves=*/false);
  EXPECT_EQ(oddTree.root, 0u);
  EXPECT_EQ(evenTree.root, 7u);
}

TEST(BuildDoubleBinaryTreeTest, ReturnsCachedInstanceForRepeatedArgs) {
  const auto& first = buildDoubleBinaryTree(8, /*oddLeaves=*/true);
  const auto& second = buildDoubleBinaryTree(8, /*oddLeaves=*/true);
  EXPECT_EQ(&first, &second);
  // Different arguments memoize to distinct instances.
  const auto& other = buildDoubleBinaryTree(8, /*oddLeaves=*/false);
  EXPECT_NE(&first, &other);
}

TEST(BuildDoubleBinaryTreeTest, OddAndEvenLeavesPartitionAllRanks) {
  // The defining double-binary-tree property: the two orientations' leaf sets
  // are disjoint and together cover every rank, so each rank is a leaf in
  // exactly one of the two trees.
  for (uint32_t n : {2u, 4u, 8u, 16u}) {
    SCOPED_TRACE(n);
    const auto& oddTree = buildDoubleBinaryTree(n, /*oddLeaves=*/true);
    const auto& evenTree = buildDoubleBinaryTree(n, /*oddLeaves=*/false);
    const std::set<uint32_t> oddLeaves(
        oddTree.leaves.begin(), oddTree.leaves.end());
    const std::set<uint32_t> evenLeaves(
        evenTree.leaves.begin(), evenTree.leaves.end());
    for (uint32_t node = 0; node < n; ++node) {
      EXPECT_NE(oddLeaves.contains(node), evenLeaves.contains(node));
    }
    EXPECT_EQ(oddLeaves.size() + evenLeaves.size(), n);
  }
}

} // namespace
} // namespace packsim
