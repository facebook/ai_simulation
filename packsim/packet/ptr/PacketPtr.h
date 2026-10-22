/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <typeindex>
#include <utility>
#include <version>

#include <folly/CPortability.h>
#include <folly/CppAttributes.h>
#include <folly/logging/xlog.h>

#include "packsim/packet/allocator/GlobalManager.h"
#include "packsim/packet/base.h"
#include "packsim/packet/type_mapper.h"

namespace packsim {

namespace detail {
/**
 * A non-movable non-copyable wrapper around the packet objects to take them
 * from global pool at request and release them when shared pointer dies.
 */
class PacketWrapper {
 private:
  /**
   * Do NOT use this constructor directly, since the pointer needs to come from
   * the global pool. Call the static method below instead.
   */
  explicit PacketWrapper(Packet* const packetPtr) : packetPtr_(packetPtr) {
#ifdef LOG_POOL_ALLOCATIONS
    XLOGF(
        INFO,
        "==> Took address {} from global pool.",
        static_cast<void*>(packetPtr_));
#endif
  }

  PacketWrapper(const PacketWrapper& other) = delete;
  PacketWrapper(PacketWrapper&& other) = delete;
  PacketWrapper& operator=(const PacketWrapper& other) = delete;
  PacketWrapper& operator=(PacketWrapper&& other) = delete;

 public:
  /**
   * Use this function to create a new wrapper with allocation from global pool.
   */
  template <PacketType packetType>
  static std::shared_ptr<PacketWrapper> make() {
    struct SharedPtrFriendlyWrapper : public PacketWrapper {
     public:
      explicit SharedPtrFriendlyWrapper(Packet* const packetPtr)
          : PacketWrapper(packetPtr) {}
    };

    return std::make_shared<SharedPtrFriendlyWrapper>(
        GlobalManager::takePacket<packetType>());
  }

  ~PacketWrapper() {
    GlobalManager::returnPacket(packetPtr_);
#ifdef LOG_POOL_ALLOCATIONS
    XLOGF(
        INFO,
        "<== Returned address {} to global pool.",
        static_cast<void*>(packetPtr_));
#endif
  }

  Packet* get() const {
    return packetPtr_;
  }

 private:
  Packet* const packetPtr_;
};
} // namespace detail

/**
 * A shared, reference-counted handle to a packet taken from the global packet
 * pool. The packet goes back to the pool when the last handle to it is
 * destroyed.
 */
class PacketPtr {
 public:
  PacketPtr() noexcept : wPtr_(nullptr) {}

  /* implicit */ PacketPtr(std::nullptr_t) noexcept : wPtr_(nullptr) {}

  explicit PacketPtr(std::shared_ptr<detail::PacketWrapper> ptr) noexcept
      : wPtr_(std::move(ptr)) {}

  PacketPtr(const PacketPtr& other) noexcept {
    wPtr_ = other.wPtr_;
  }

  PacketPtr(PacketPtr&& other) noexcept {
    wPtr_ = std::move(other.wPtr_);
  }

  PacketPtr& operator=(const PacketPtr& other) noexcept {
    wPtr_ = other.wPtr_;
    return *this;
  }

  PacketPtr& operator=(PacketPtr&& other) noexcept {
    wPtr_ = std::move(other.wPtr_);
    return *this;
  }

  ~PacketPtr() = default;

  packsim::Packet* operator->() const noexcept {
    return get();
  }

  packsim::Packet& operator*() const noexcept {
    return *get();
  }

  bool operator==(const PacketPtr& other) const noexcept {
    return get() == other.get();
  }

  bool operator==(std::nullptr_t) const noexcept {
    return get() == nullptr;
  }

  /**
   * Cast the internal pointer as pointer of another type using C++ type.
   *
   * NOTE: This function will crash if the internal pointer is NULL, only call
   * this on pointers that have been loaded with a packet.
   */
  template <typename PacketCppType, bool UseDynamicCast = false>
  PacketCppType* FOLLY_NONNULL as() const {
    Packet* ptr = get();
    if constexpr (getPacketTypeEnum<PacketCppType>() == PacketType::RAW) {
      return ptr;
    }
    if constexpr (UseDynamicCast) {
      return dynamic_cast<PacketCppType*>(ptr);
    }
    return static_cast<PacketCppType*>(ptr);
  }

  /**
   * Whether this holds a `PacketCppType` or something derived from one.
   *
   * Answered from the packet's own `pktType` via `PacketTypeMatcher`, not a
   * `dynamic_cast`, so it is cheap enough for the per-frame paths. A null
   * `PacketPtr` is nothing, including nothing derived from `Packet`.
   */
  template <typename PacketCppType>
  [[nodiscard]] bool is() const noexcept {
    Packet* ptr = get();
    return ptr != nullptr &&
        PacketTypeMatcher<PacketCppType>::matches(ptr->pktType);
  }

  /**
   * The packet as a `PacketCppType*`, or nullptr when it is not one.
   *
   * The nullable counterpart of `as()`, which is `FOLLY_NONNULL` and must only
   * be called once the type is known. Replaces writing the test and the cast
   * separately.
   */
  template <typename PacketCppType>
  [[nodiscard]] PacketCppType* tryAs() const noexcept {
    Packet* ptr = get();
    if (ptr != nullptr &&
        PacketTypeMatcher<PacketCppType>::matches(ptr->pktType)) {
      return static_cast<PacketCppType*>(ptr);
    }
    return nullptr;
  }

  /**
   * Cast the internal pointer as pointer of another type using enum.
   *
   * NOTE: This function will crash if the internal pointer is NULL, only call
   * this on pointers that have been loaded with a packet.
   */
  template <
      PacketType packetType,
      bool UseDynamicCast = true,
      typename PacketCppType = typename PacketTypeMapper<packetType>::type>
  PacketCppType* FOLLY_NONNULL as() const {
    Packet* ptr = get();
    if constexpr (packetType == PacketType::RAW) {
      return ptr;
    }
    if constexpr (UseDynamicCast) {
      return dynamic_cast<PacketCppType*>(ptr);
    }
    return static_cast<PacketCppType*>(ptr);
  }

  bool operator!() const noexcept {
    auto ptr = wPtr_.get();
    return (ptr == nullptr);
  }

  FOLLY_ALWAYS_INLINE packsim::Packet* get() const noexcept {
    auto ptr = wPtr_.get();
    return (ptr == nullptr) ? nullptr : ptr->get();
  }

 private:
  std::shared_ptr<detail::PacketWrapper> wPtr_;
};

/**
 * Use this to create a new packet loaded from global pool.
 */
template <PacketType packetType>
static PacketPtr createPacket(uint32_t const payloadSize = 0) {
  auto pktPtr = PacketPtr(detail::PacketWrapper::make<packetType>());
  pktPtr.as<packetType, /*UseDynamicCast=*/false>()->reset();
  pktPtr->payloadSize = payloadSize;
  pktPtr->pktType = packetType;
  return pktPtr;
}

} // namespace packsim

template <>
struct std::hash<packsim::PacketPtr> {
  std::size_t operator()(const packsim::PacketPtr& ptr) const noexcept {
    return std::hash<packsim::Packet*>{}(ptr.get());
  }
};
