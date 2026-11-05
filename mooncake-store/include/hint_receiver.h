#pragma once

// HintReceiver: an inbound ZMQ PULL socket for KV cache hints from an external
// controller, such as a KV router. Hints are fire and forget: nothing is sent
// back, a malformed one is counted and dropped, and a sender that outruns the
// master drops at its own high-water mark rather than block.
//
// Each message is one msgpack array whose first element names the hint:
//
//     ["Pin", tenant_id, [key, ...], ttl_ms, priority]
//
// See MasterService::PinObjects for what a pin does.

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace mooncake {

class HintReceiver {
   public:
    struct PinHint {
        std::string tenant_id;
        std::vector<std::string> keys;
        uint64_t ttl_ms{0};
        uint32_t priority{0};
    };
    using PinHandler = std::function<void(const PinHint&)>;

    // A message carries at most this many keys, as /api/v1/soft_pin does.
    static constexpr size_t kMaxKeysPerHint = 4096;

    HintReceiver(std::string endpoint, PinHandler on_pin);
    ~HintReceiver();

    HintReceiver(const HintReceiver&) = delete;
    HintReceiver& operator=(const HintReceiver&) = delete;

    // Binds the socket and starts receiving. False if the bind fails or ZMQ
    // was not built in (ENABLE_KV_EVENTS=OFF).
    bool Start();
    void Stop();

    // Decodes one message into a pin; false if it is not a well-formed one.
    static bool DecodePin(std::string_view message, PinHint* hint);

    uint64_t received() const { return received_.load(); }
    uint64_t malformed() const { return malformed_.load(); }

   private:
    void Run();

    const std::string endpoint_;
    const PinHandler on_pin_;
    void* context_{nullptr};
    void* socket_{nullptr};
    std::thread thread_;
    std::atomic<bool> stopping_{false};
    std::atomic<uint64_t> received_{0};
    std::atomic<uint64_t> malformed_{0};
};

}  // namespace mooncake
