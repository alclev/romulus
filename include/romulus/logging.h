#pragma once

#include <cstdio>
#include <format>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

namespace romulus {

/// @brief An enum to track the type of status
///
/// TODO: This doesn't need so much engineering.  Why isn't a Variant good
///       enough?  Does the type of error really matter?
enum StatusType {
  Ok,             // TODO
  InternalError,  // TODO
  Unavailable,    // TODO
  Aborted,        // TODO
};

/// @brief A status object that can be used to track the status of an operation
///
/// TODO: I'm wondering why this can't just be a variant... it's either OK or
/// Some(string)...
struct Status {
  StatusType t;                        // TODO
  std::optional<std::string> message;  // TODO

  /// @brief Create a Status object with the given type and message
  /// @return A Status object with the given type and message
  static Status Ok() { return {StatusType::Ok, {}}; }

  /// @brief Define the operator << for Status
  /// @tparam T The type of the object to append to the message
  /// @param t The object itself to append to the message
  /// @return A Status object with the appended message
  template <typename T>
  Status operator<<(T t) {
    std::string curr = message ? message.value() : "";
    std::stringstream s;
    s << curr;
    s << t;
    message = s.str();
    return *this;
  }
};

/// @brief A simple struct that contains the status with its value
/// @tparam T The type of the value
///
/// TODO: We might be able to get by with a std::variant.
template <class T>
struct StatusVal {
  Status status;         // TODO
  std::optional<T> val;  // TODO
};
}  // namespace romulus

namespace romulus {
#define RELEASE 0
#define DEBUG 1

// Make sure we have a log level, even if the build tools didn't define one
#ifndef ROMULUS_LOG_LEVEL
#warning "ROMULUS_LOG_LEVEL is not defined... defaulting to DEBUG"
#define ROMULUS_LOG_LEVEL DEBUG
#endif
#if ROMULUS_LOG_LEVEL != RELEASE && ROMULUS_LOG_LEVEL != DEBUG
#warning "Invalid value for ROMULUS_LOG_LEVEL.  Defaulting to DEBUG"
#define ROMULUS_LOG_LEVEL DEBUG
#endif

/// Print a message
///
/// @param msg The message to print
///
inline void print_debug(std::string_view msg, const char *file, uint32_t line) {
  // NB: for thread-safety, we use printf
  std::printf("[DEBUG] %.*s (%s:%u)\n", (int)msg.length(), msg.data(), file,
              line);
  std::fflush(stdout);
}
/// Print an info message
///
/// @param msg The message to print
inline void print_info(std::string_view msg) {
  // NB: for thread-safety, we use printf
  std::printf("[INFO] %.*s\n", (int)msg.length(), msg.data());
  std::fflush(stdout);
}
/// Print a fatal message
///
/// @param msg The message to print
inline void print_fatal(std::string_view msg) {
  // NB: for thread-safety, we use printf
  std::printf("[FATAL] %.*s\n", (int)msg.length(), msg.data());
  std::fflush(stdout);
}

/// Print a debug message only if ROMULUS_DEBUG_MSGS is defined
#if ROMULUS_LOG_LEVEL == DEBUG
#define ROMULUS_DEBUG(...) \
  romulus::print_debug(std::format(__VA_ARGS__), __FILE__, __LINE__)
#else
#define ROMULUS_DEBUG(...)
#endif

/// Print an information message
#define ROMULUS_INFO(...) romulus::print_info(std::format(__VA_ARGS__))

/// Terminate with a message on a fatal error
#define ROMULUS_FATAL(...)                          \
  {                                                 \
    romulus::print_fatal(std::format(__VA_ARGS__)); \
    std::_Exit(1);                                  \
  }

/// Assert, and print a fatal message if it fails
///
/// TODO: ASSERT doesn't print a line number or file number.  We should add that
///       (see DEBUG).
#define ROMULUS_ASSERT(check, ...)                  \
  if (!(check)) [[unlikely]] {                      \
    romulus::print_fatal(std::format(__VA_ARGS__)); \
    std::_Exit(1);                                  \
  }

/// Terminate if status is not OK
#define OK_OR_FAIL(status)                                            \
  if (auto __s = status; (__s.t != romulus::util::Ok)) [[unlikely]] { \
    ROMULUS_FATAL("{}", __s.message.value());                         \
  }

/// Fail if func does not return 0
#define RDMA_CM_ASSERT(func, ...)                                       \
  {                                                                     \
    int ret = func(__VA_ARGS__);                                        \
    ROMULUS_ASSERT(ret == 0, "{}{}{}", #func, "(): ", strerror(errno)); \
  }

/// TODO
inline void INIT() {
#if ROMULUS_LOG_LEVEL == DEBUG
  std::printf("ROMULUS::DEBUG is true\n");
#else
  std::printf("ROMULUS::DEBUG is false\n");
#endif
}
}  // namespace romulus
