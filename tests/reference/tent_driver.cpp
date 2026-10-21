// Test-only adapter to the frozen TENT C ABI. No Mooncake code is vendored.
#include <algorithm>
#include <charconv>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "tent/transfer_engine.h"

namespace {
void require(bool condition, const std::string &message) {
  if (!condition)
    throw std::runtime_error(message);
}

void checked(int result, const std::string &operation) {
  require(result == 0, "TENT failed: " + operation);
}

size_t number(const std::string &value) {
  size_t result = 0;
  auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  require(error == std::errc{} && end == value.data() + value.size(),
          "invalid integer");
  return result;
}

std::vector<std::string> split(const std::string &value, char separator) {
  std::istringstream input(value);
  std::vector<std::string> result;
  for (std::string field; std::getline(input, field, separator);)
    result.push_back(field);
  return result;
}

std::string text(const std::string &value) { return value == "-" ? "" : value; }

std::string hex(const void *data, size_t length) {
  if (length == 0)
    return "-";
  std::ostringstream result;
  const auto *bytes = static_cast<const unsigned char *>(data);
  for (size_t i = 0; i < length; ++i)
    result << std::hex << std::setfill('0') << std::setw(2)
           << unsigned(bytes[i]);
  return result.str();
}

void range(size_t offset, size_t length, size_t capacity) {
  require(length > 0 && offset <= capacity && length <= capacity - offset,
          "invalid buffer range");
}

using Engine = std::unique_ptr<void, decltype(&tent_destroy_engine)>;

Engine engine() {
  Engine result(tent_create_engine(), tent_destroy_engine);
  require(result && tent_available(result.get()) > 0,
          "TENT engine unavailable");
  return result;
}

struct Buffer {
  std::vector<unsigned char> bytes;
  size_t references = 0;
};

class Trace {
  // Engines are destroyed before backing even on a failed operation.
  std::map<std::string, Buffer> buffers_;
  Engine source_{engine()};
  Engine requester_{engine()};
  std::optional<tent_segment_id_t> peer_;
  tent_notifi_info poll_{};

  void *owner(const std::string &name) {
    require(name == "source" || name == "local", "unknown buffer owner");
    return name == "source" ? source_.get() : requester_.get();
  }

  void describe(const std::string &label) {
    if (peer_)
      checked(tent_close_segment(requester_.get(), *peer_), "close segment");
    char name[256]{};
    checked(tent_segment_name(source_.get(), name, sizeof(name)),
            "segment name");
    tent_segment_id_t peer = 0;
    checked(tent_open_segment(requester_.get(), &peer, name), "open segment");
    require(peer != LOCAL_SEGMENT_ID, "remote peer resolved to self");
    peer_ = peer;
    tent_segment_info_t info{};
    checked(tent_get_segment_info(requester_.get(), peer, &info), "describe");
    require(info.num_buffers >= 0 && info.num_buffers <= 1,
            "unexpected descriptor count");
    size_t length = 0;
    for (int i = 0; i < info.num_buffers; ++i) {
      require(info.buffers != nullptr, "missing descriptor storage");
      const auto &buffer = buffers_.at("source").bytes;
      require(info.buffers[i].base ==
                      reinterpret_cast<uint64_t>(buffer.data()) &&
                  info.buffers[i].length == buffer.size(),
              "descriptor does not identify actual backing");
      length += info.buffers[i].length;
    }
    std::cout << "OBS\tDESCRIBE\t" << label << '\t' << info.num_buffers << '\t'
              << length << '\n';
    tent_free_segment_info(&info);
  }

  void transfer(const std::vector<std::string> &fields) {
    require(peer_.has_value(), "DESCRIBE must precede transfers");
    auto slices =
        fields[2] == "-" ? std::vector<std::string>{} : split(fields[2], ',');
    std::vector<tent_request_t> entries(std::max<size_t>(1, slices.size()));
    for (size_t i = 0; i < slices.size(); ++i) {
      auto parts = split(slices[i], ':');
      require(parts.size() == 3, "invalid slice");
      const auto local = number(parts[0]), remote = number(parts[1]),
                 length = number(parts[2]);
      auto &source = buffers_.at("source").bytes;
      auto &destination = buffers_.at("local").bytes;
      range(local, length, destination.size());
      range(remote, length, source.size());
      entries[i].opcode = fields[0] == "READ" ? OPCODE_READ : OPCODE_WRITE;
      entries[i].source = destination.data() + local;
      entries[i].target_id = *peer_;
      entries[i].target_offset =
          reinterpret_cast<uint64_t>(source.data() + remote);
      entries[i].length = length;
      entries[i].transport_hint = TRANSPORT_TCP;
    }
    auto batch = tent_allocate_batch(requester_.get(), entries.size());
    require(batch != 0, "batch allocation failed");
    try {
      auto name = text(fields[3]), message = text(fields[4]);
      if (fields[3] == "~")
        checked(
            tent_submit(requester_.get(), batch, entries.data(), slices.size()),
            "submit");
      else
        checked(tent_submit_notif(requester_.get(), batch, entries.data(),
                                  slices.size(), name.c_str(), message.c_str()),
                "submit notification");
      tent_status_t status{};
      const auto end =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      do {
        checked(tent_overall_status(requester_.get(), batch, &status),
                "batch status");
        if (status.status == STATUS_COMPLETED)
          break;
        require(status.status == STATUS_PENDING ||
                    status.status == STATUS_WAITING,
                "transfer reached failed terminal state");
        require(std::chrono::steady_clock::now() < end,
                "transfer deadline expired");
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      } while (true);
      for (int i = 0; i < 2; ++i) {
        tent_status_t repeated{};
        checked(tent_overall_status(requester_.get(), batch, &repeated),
                "repeat status");
        require(repeated.status == status.status &&
                    repeated.transferred_bytes == status.transferred_bytes,
                "terminal status changed");
      }
      std::cout << "OBS\tRESULT\t" << fields[1] << "\tcompleted\t"
                << slices.size() << '\t' << status.transferred_bytes << '\n';
    } catch (...) {
      tent_free_batch(requester_.get(), batch);
      throw;
    }
    checked(tent_free_batch(requester_.get(), batch), "free batch");
  }

  void take(const std::string &label) {
    checked(tent_recv_notifs(source_.get(), &poll_), "receive notifications");
    require(poll_.num_records >= 0 &&
                (poll_.num_records == 0 ? poll_.records == nullptr
                                        : poll_.records != nullptr),
            "invalid notification output");
    std::cout << "OBS\tTAKE\t" << label << '\t' << poll_.num_records << '\t';
    for (int i = 0; i < poll_.num_records; ++i) {
      if (i)
        std::cout << ',';
      const auto &record = poll_.records[i];
      std::string name(record.name), message(record.msg);
      std::cout << hex(name.data(), name.size()) << ':'
                << hex(message.data(), message.size());
    }
    if (!poll_.num_records)
      std::cout << '-';
    std::cout << '\n';
    tent_free_notifs(&poll_);
    require(poll_.num_records == 0 && poll_.records == nullptr,
            "notification free did not reset output");
    tent_free_notifs(&poll_);
  }

public:
  ~Trace() { tent_free_notifs(&poll_); }

  void execute(const std::vector<std::string> &fields) {
    const auto &operation = fields.at(0);
    if (operation == "ORBITTE_CPU_TRACE" && fields.size() == 2 &&
        fields[1] == "1") {
      std::cout << "OBS\tVERSION\t1\n";
    } else if (operation == "BUFFER" && fields.size() == 4) {
      const auto length = number(fields[2]);
      require(length > 0 && length <= (1 << 20) &&
                  !buffers_.contains(fields[1]),
              "invalid buffer");
      owner(fields[1]);
      Buffer buffer{std::vector<unsigned char>(length)};
      const auto initial = fields[3] == "pattern" ? 0 : number(fields[3]);
      require(initial <= 255, "invalid fill byte");
      for (size_t i = 0; i < length; ++i)
        buffer.bytes[i] = fields[3] == "pattern" ? i % 251 : initial;
      buffers_.emplace(fields[1], std::move(buffer));
    } else if (operation == "REGISTER" && fields.size() == 3) {
      auto &buffer = buffers_.at(fields[1]);
      int permission;
      if (fields[2] == "rw")
        permission = PERM_GLOBAL_READ_WRITE;
      else if (fields[2] == "r")
        permission = PERM_GLOBAL_READ_ONLY;
      else {
        require(fields[2] == "local", "unknown permission");
        permission = PERM_LOCAL_READ_WRITE;
      }
      checked(tent_register_memory_with_perm(owner(fields[1]),
                                             buffer.bytes.data(),
                                             buffer.bytes.size(), permission),
              "register");
      ++buffer.references;
    } else if (operation == "REGISTER_BATCH" && fields.size() == 4) {
      auto &buffer = buffers_.at(fields[1]);
      const auto count = number(fields[3]);
      require(fields[2] == "r" && count > 0 && count <= 1024 &&
                  buffer.references > 0,
              "invalid registration batch");
      std::vector<void *> addresses(count, buffer.bytes.data());
      std::vector<size_t> lengths(count, buffer.bytes.size());
      checked(tent_register_memory_batch(owner(fields[1]), addresses.data(),
                                         lengths.data(), count,
                                         PERM_GLOBAL_READ_ONLY),
              "register batch");
      buffer.references += count;
      std::cout << "OBS\tREGISTER_BATCH\t" << fields[1] << '\t' << count
                << '\n';
    } else if (operation == "UNREGISTER" && fields.size() == 2) {
      auto &buffer = buffers_.at(fields[1]);
      require(buffer.references > 0, "buffer not registered");
      checked(tent_unregister_memory(owner(fields[1]), buffer.bytes.data(),
                                     buffer.bytes.size()),
              "unregister");
      --buffer.references;
    } else if (operation == "DESCRIBE" && fields.size() == 2) {
      describe(fields[1]);
    } else if (operation == "FILL" && fields.size() == 5) {
      auto &buffer = buffers_.at(fields[1]).bytes;
      const auto offset = number(fields[2]), length = number(fields[3]),
                 value = number(fields[4]);
      range(offset, length, buffer.size());
      require(value <= 255, "invalid fill byte");
      std::fill_n(buffer.begin() + offset, length, value);
    } else if (operation == "BYTES" && fields.size() == 5) {
      const auto &buffer = buffers_.at(fields[2]).bytes;
      const auto offset = number(fields[3]), length = number(fields[4]);
      range(offset, length, buffer.size());
      std::cout << "OBS\tBYTES\t" << fields[1] << '\t'
                << hex(buffer.data() + offset, length) << '\n';
    } else if ((operation == "READ" || operation == "WRITE") &&
               fields.size() == 5) {
      transfer(fields);
    } else if (operation == "NOTIFY" && fields.size() == 5) {
      require(fields[2] == "local" || (fields[2] == "remote" && peer_),
              "unknown notification route");
      auto name = text(fields[3]), message = text(fields[4]);
      checked(tent_send_notifs(fields[2] == "local" ? source_.get()
                                                    : requester_.get(),
                               fields[2] == "local" ? LOCAL_SEGMENT_ID : *peer_,
                               name.c_str(), message.c_str()),
              "send notification");
      std::cout << "OBS\tNOTIFY\t" << fields[1] << "\tok\n";
    } else if (operation == "TAKE" && fields.size() == 2) {
      take(fields[1]);
    } else {
      throw std::runtime_error("invalid trace instruction");
    }
  }

  void finish() {
    for (const auto &[name, buffer] : buffers_)
      require(buffer.references == 0,
              "trace did not unregister all references");
    if (peer_)
      checked(tent_close_segment(requester_.get(), *peer_),
              "close final segment");
    requester_.reset();
    source_.reset();
    std::cout << "OBS\tCLEANUP\tok\n";
  }
};
} // namespace

int main(int argc, char **argv) {
  try {
    require(argc == 2, "expected trace path");
    tent_set_config("metadata_type", "p2p");
    tent_set_config("rpc_server_hostname", "127.0.0.1");
    tent_set_config("rpc_server_port", "0");
    tent_set_config("rpc_server_threads", "4");
    tent_set_config("log_level", "error");
    tent_set_config("enable_progress_worker", "false");
    tent_set_config("enable_runtime_queue", "false");
    tent_set_config("merge_requests", "false");
    for (const auto *transport :
         {"tcp", "rdma", "shm", "hp_tcp", "io_uring", "nvlink", "mnnvl", "gds",
          "ascend_direct", "sunrise_link", "mpcomm", "tpu", "ub", "xpu",
          "hylink", "fabric", "mlu", "memfabric"}) {
      const auto key = std::string("transports/") + transport + "/enable";
      tent_set_config(key.c_str(),
                      std::string(transport) == "tcp" ? "true" : "false");
    }
    std::ifstream input(argv[1]);
    require(input.good(), "cannot open trace");
    Trace trace;
    for (std::string line; std::getline(input, line);) {
      std::istringstream stream(line);
      std::vector<std::string> fields;
      for (std::string field; stream >> field;)
        fields.push_back(field);
      if (fields.empty() || fields[0].starts_with('#'))
        continue;
      trace.execute(fields);
    }
    trace.finish();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
