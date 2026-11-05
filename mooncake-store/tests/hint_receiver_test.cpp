#include "hint_receiver.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#ifdef MOONCAKE_ENABLE_KV_EVENTS
#include <msgpack.hpp>
#include <zmq.h>
#endif

namespace mooncake {
namespace {

#ifdef MOONCAKE_ENABLE_KV_EVENTS

std::string PackPin(const std::string& tag, const std::string& tenant,
                    const std::vector<std::string>& keys, uint64_t ttl_ms,
                    uint32_t priority) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);
    packer.pack_array(5);
    packer.pack(tag);
    packer.pack(tenant);
    packer.pack(keys);
    packer.pack(ttl_ms);
    packer.pack(priority);
    return {buffer.data(), buffer.size()};
}

TEST(HintReceiverTest, DecodesOnlyWellFormedPins) {
    HintReceiver::PinHint hint;
    ASSERT_TRUE(HintReceiver::DecodePin(
        PackPin("Pin", "default", {"k1", "k2"}, 60000, 3), &hint));
    EXPECT_EQ(hint.tenant_id, "default");
    EXPECT_EQ(hint.keys, (std::vector<std::string>{"k1", "k2"}));
    EXPECT_EQ(hint.ttl_ms, 60000u);
    EXPECT_EQ(hint.priority, 3u);

    EXPECT_FALSE(HintReceiver::DecodePin(
        PackPin("Prefetch", "default", {"k1"}, 1, 0), &hint));
    EXPECT_FALSE(
        HintReceiver::DecodePin(PackPin("Pin", "default", {}, 1, 0), &hint));
    std::vector<std::string> too_many(HintReceiver::kMaxKeysPerHint + 1, "k");
    EXPECT_FALSE(HintReceiver::DecodePin(
        PackPin("Pin", "default", too_many, 1, 0), &hint));
    EXPECT_FALSE(HintReceiver::DecodePin("not msgpack at all", &hint));

    msgpack::sbuffer short_array;
    msgpack::packer<msgpack::sbuffer> packer(short_array);
    packer.pack_array(2);
    packer.pack(std::string("Pin"));
    packer.pack(std::string("default"));
    EXPECT_FALSE(HintReceiver::DecodePin(
        {short_array.data(), short_array.size()}, &hint));

    // A key list of the wrong type.
    msgpack::sbuffer wrong_type;
    msgpack::packer<msgpack::sbuffer> wrong(wrong_type);
    wrong.pack_array(5);
    wrong.pack(std::string("Pin"));
    wrong.pack(std::string("default"));
    wrong.pack(42);
    wrong.pack(1);
    wrong.pack(0);
    EXPECT_FALSE(
        HintReceiver::DecodePin({wrong_type.data(), wrong_type.size()}, &hint));
}

TEST(HintReceiverTest, DeliversPinsAndDropsMalformedOverTheSocket) {
    const std::string endpoint =
        "ipc:///tmp/mooncake-hint-test-" + std::to_string(::getpid());
    std::mutex mutex;
    std::vector<HintReceiver::PinHint> pins;
    HintReceiver receiver(endpoint, [&](const HintReceiver::PinHint& hint) {
        std::lock_guard<std::mutex> lock(mutex);
        pins.push_back(hint);
    });
    ASSERT_TRUE(receiver.Start());

    void* context = zmq_ctx_new();
    void* push = zmq_socket(context, ZMQ_PUSH);
    ASSERT_EQ(zmq_connect(push, endpoint.c_str()), 0);
    const std::string garbage = "garbage";
    const std::string pin = PackPin("Pin", "default", {"a", "b"}, 5000, 2);
    ASSERT_GE(zmq_send(push, garbage.data(), garbage.size(), 0), 0);
    ASSERT_GE(zmq_send(push, pin.data(), pin.size(), 0), 0);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (receiver.received() < 2 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(receiver.received(), 2u);
    EXPECT_EQ(receiver.malformed(), 1u);
    {
        std::lock_guard<std::mutex> lock(mutex);
        ASSERT_EQ(pins.size(), 1u);
        EXPECT_EQ(pins[0].keys, (std::vector<std::string>{"a", "b"}));
        EXPECT_EQ(pins[0].priority, 2u);
    }

    const int linger = 0;
    zmq_setsockopt(push, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_close(push);
    zmq_ctx_term(context);
    receiver.Stop();
}

#else

TEST(HintReceiverTest, NeedsZmq) {
    HintReceiver receiver("ipc:///tmp/unused", [](const auto&) {});
    EXPECT_FALSE(receiver.Start());
}

#endif

}  // namespace
}  // namespace mooncake
