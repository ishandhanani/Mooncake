#include "hint_receiver.h"

#include <glog/logging.h>

#ifdef MOONCAKE_ENABLE_KV_EVENTS
#include <msgpack.hpp>
#include <zmq.h>
#endif

#include <utility>

namespace mooncake {

HintReceiver::HintReceiver(std::string endpoint, PinHandler on_pin)
    : endpoint_(std::move(endpoint)), on_pin_(std::move(on_pin)) {}

HintReceiver::~HintReceiver() { Stop(); }

#ifdef MOONCAKE_ENABLE_KV_EVENTS

bool HintReceiver::DecodePin(std::string_view message, PinHint* hint) {
    try {
        const msgpack::object_handle handle =
            msgpack::unpack(message.data(), message.size());
        const msgpack::object& root = handle.get();
        if (root.type != msgpack::type::ARRAY || root.via.array.size != 5) {
            return false;
        }
        const msgpack::object* fields = root.via.array.ptr;
        if (fields[0].as<std::string>() != "Pin") {
            return false;
        }
        hint->tenant_id = fields[1].as<std::string>();
        hint->keys = fields[2].as<std::vector<std::string>>();
        hint->ttl_ms = fields[3].as<uint64_t>();
        hint->priority = fields[4].as<uint32_t>();
    } catch (const std::exception&) {
        return false;
    }
    return !hint->keys.empty() && hint->keys.size() <= kMaxKeysPerHint;
}

bool HintReceiver::Start() {
    context_ = zmq_ctx_new();
    socket_ = context_ == nullptr ? nullptr : zmq_socket(context_, ZMQ_PULL);
    if (socket_ == nullptr) {
        LOG(ERROR) << "hint socket: cannot create a ZMQ socket: "
                   << zmq_strerror(zmq_errno());
        Stop();
        return false;
    }
    const int linger = 0;
    const int hwm = 100000;
    zmq_setsockopt(socket_, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(socket_, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    if (zmq_bind(socket_, endpoint_.c_str()) != 0) {
        LOG(ERROR) << "hint socket: cannot bind " << endpoint_ << ": "
                   << zmq_strerror(zmq_errno());
        Stop();
        return false;
    }
    LOG(INFO) << "hint socket: listening on " << endpoint_;
    thread_ = std::thread([this] { Run(); });
    return true;
}

void HintReceiver::Stop() {
    stopping_.store(true);
    if (thread_.joinable()) {
        thread_.join();
    }
    if (socket_ != nullptr) {
        zmq_close(socket_);
        socket_ = nullptr;
    }
    if (context_ != nullptr) {
        zmq_ctx_term(context_);
        context_ = nullptr;
    }
}

void HintReceiver::Run() {
    zmq_pollitem_t item{socket_, 0, ZMQ_POLLIN, 0};
    while (!stopping_.load()) {
        // Wakes up to notice Stop.
        if (zmq_poll(&item, 1, 200) <= 0 || !(item.revents & ZMQ_POLLIN)) {
            continue;
        }
        zmq_msg_t message;
        zmq_msg_init(&message);
        while (zmq_msg_recv(&message, socket_, ZMQ_DONTWAIT) >= 0) {
            received_.fetch_add(1);
            PinHint hint;
            if (DecodePin({static_cast<const char*>(zmq_msg_data(&message)),
                           zmq_msg_size(&message)},
                          &hint)) {
                on_pin_(hint);
            } else {
                malformed_.fetch_add(1);
                LOG_EVERY_N(WARNING, 1000)
                    << "hint socket: dropped a malformed hint ("
                    << malformed_.load() << " so far)";
            }
        }
        zmq_msg_close(&message);
    }
}

#else  // MOONCAKE_ENABLE_KV_EVENTS

bool HintReceiver::DecodePin(std::string_view, PinHint*) { return false; }

bool HintReceiver::Start() {
    LOG(ERROR) << "hint socket: built without ZMQ; rebuild with "
                  "-DENABLE_KV_EVENTS=ON";
    return false;
}

void HintReceiver::Stop() {}

void HintReceiver::Run() {}

#endif  // MOONCAKE_ENABLE_KV_EVENTS

}  // namespace mooncake
