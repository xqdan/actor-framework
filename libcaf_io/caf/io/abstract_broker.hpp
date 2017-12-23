/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2017                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_IO_ABSTRACT_BROKER_HPP
#define CAF_IO_ABSTRACT_BROKER_HPP

#include <vector>
#include <unordered_map>

#include "caf/scheduled_actor.hpp"
#include "caf/prohibit_top_level_spawn_marker.hpp"
#include "caf/detail/intrusive_partitioned_list.hpp"

#include "caf/io/fwd.hpp"
#include "caf/io/accept_handle.hpp"
#include "caf/io/receive_policy.hpp"
#include "caf/io/system_messages.hpp"
#include "caf/io/connection_handle.hpp"

#include "caf/io/network/native_socket.hpp"
#include "caf/io/network/stream_manager.hpp"
#include "caf/io/network/acceptor_manager.hpp"

namespace caf {
namespace io {

class middleman;

/// @defgroup Broker Actor-based Network Abstraction
///
/// Brokers provide an actor-based abstraction for low-level network IO.
/// The central component in the network abstraction of CAF is the
/// `middleman`. It connects any number of brokers to a `multiplexer`,
/// which implements a low-level IO event loop.
///
/// ![Relation between middleman, multiplexer, and broker](broker.png)
///
/// Brokers do *not* operate on sockets or other platform-dependent
/// communication primitives. Instead, brokers use a `connection_handle`
/// to identify a reliable, end-to-end byte stream (e.g. a TCP connection)
/// and `accept_handle` to identify a communication endpoint others can
/// connect to via its port.
///
/// Each `connection_handle` is associated with a `scribe` that provides
/// access to an output buffer as well as a `flush` operation to request
/// sending its content via the network. Instead of actively receiving data,
/// brokers configure a scribe to asynchronously receive data, e.g.,
/// `self->configure_read(hdl, receive_policy::exactly(1024))` would
/// configure the scribe associated to `hdl` to receive *exactly* 1024 bytes
/// and generate a `new_data_msg` message for the broker once the
/// data is available. The buffer in this message will be re-used by the
/// scribe to minimize memory usage and heap allocations.
///
/// Each `accept_handle` is associated with a `doorman` that will create
/// a `new_connection_msg` whenever a new connection was established.
///
/// All `scribe` and `doorman` instances are managed by the `multiplexer`

/// A broker mediates between actor systems and other components in the network.
/// @ingroup Broker
class abstract_broker : public scheduled_actor,
                        public prohibit_top_level_spawn_marker {
public:
  ~abstract_broker() override;

  // even brokers need friends
  friend class scribe;
  friend class doorman;

  // -- overridden modifiers of abstract_actor ---------------------------------

  void enqueue(mailbox_element_ptr, execution_unit*) override;

  void enqueue(strong_actor_ptr, message_id, message, execution_unit*) override;

  // -- overridden modifiers of local_actor ------------------------------------

  void launch(execution_unit* eu, bool lazy, bool hide) override;

  // -- overridden modifiers of abstract_broker --------------------------------

  bool cleanup(error&& reason, execution_unit* host) override;

  // -- overridden modifiers of resumable --------------------------------------

  resume_result resume(execution_unit*, size_t) override;

  // -- modifiers --------------------------------------------------------------

  /// Suspends activities on `hdl` unconditionally.
  template <class Handle>
  void halt(Handle hdl) {
    auto ref = by_id(hdl);
    if (ref)
      ref->halt();
  }

  /// Allows activities on `hdl` unconditionally (default).
  template <class Handle>
  void trigger(Handle hdl) {
    auto ref = by_id(hdl);
    if (ref)
      ref->trigger();
  }

  /// Allows `num_events` activities on `hdl`.
  template <class Handle>
  void trigger(Handle hdl, size_t num_events) {
    auto ref = by_id(hdl);
    if (!ref)
      return;
    if (num_events > 0) {
      ref->trigger(num_events);
    } else {
      // if we have any number of activity tokens, ignore this call
      // otherwise (currently in unconditional receive state) halt
      auto x = ref->activity_tokens();
      if (!x)
        ref->halt();
    }
  }

  /// Modifies the receive policy for given connection.
  /// @param hdl Identifies the affected connection.
  /// @param cfg Contains the new receive policy.
  void configure_read(connection_handle hdl, receive_policy::config cfg);

  /// Enables or disables write notifications for given connection.
  void ack_writes(connection_handle hdl, bool enable);

  /// Returns the write buffer for given connection.
  std::vector<char>& wr_buf(connection_handle hdl);

  /// Writes `data` into the buffer for given connection.
  void write(connection_handle hdl, size_t bs, const void* buf);

  /// Sends the content of the buffer for given connection.
  void flush(connection_handle hdl);

  /// Returns the middleman instance this broker belongs to.
  inline middleman& parent() {
    return system().middleman();
  }

  /// Adds the unitialized `scribe` instance `ptr` to this broker.
  void add_scribe(scribe_ptr ptr);

  /// Creates and assigns a new `scribe` from given native socked `fd`.
  connection_handle add_scribe(network::native_socket fd);

  /// Tries to connect to `host` on given `port` and creates
  /// a new scribe describing the connection afterwards.
  /// @returns The handle of the new `scribe` on success.
  expected<connection_handle> add_tcp_scribe(const std::string& host,
                                             uint16_t port);

  /// Moves the initialized `scribe` instance `ptr` from another broker to this
  /// broker.
  void move_scribe(scribe_ptr ptr);

  /// Adds a `doorman` instance to this broker.
  void add_doorman(doorman_ptr ptr);

  /// Creates and assigns a new `doorman` from given native socked `fd`.
  accept_handle add_doorman(network::native_socket fd);

  /// Adds a `doorman` instance to this broker.
  void move_doorman(doorman_ptr ptr);

  /// Tries to open a local port and creates a `doorman` managing
  /// it on success. If `port == 0`, then the broker will ask
  /// the operating system to pick a random port.
  /// @returns The handle of the new `doorman` and the assigned port.
  expected<std::pair<accept_handle, uint16_t>>
  add_tcp_doorman(uint16_t port = 0, const char* in = nullptr,
                  bool reuse_addr = false);

  /// Returns the remote address associated to `hdl`
  /// or empty string if `hdl` is invalid.
  std::string remote_addr(connection_handle hdl);

  /// Returns the remote port associated to `hdl`
  /// or `0` if `hdl` is invalid.
  uint16_t remote_port(connection_handle hdl);

  /// Returns the local address associated to `hdl`
  /// or empty string if `hdl` is invalid.
  std::string local_addr(accept_handle hdl);

  /// Returns the local port associated to `hdl` or `0` if `hdl` is invalid.
  uint16_t local_port(accept_handle hdl);

  /// Returns the handle associated to given local `port` or `none`.
  accept_handle hdl_by_port(uint16_t port);

  /// Closes all connections and acceptors.
  void close_all();

  /// Closes the connection or acceptor identified by `handle`.
  /// Unwritten data will still be send.
  template <class Handle>
  bool close(Handle hdl) {
    auto x = by_id(hdl);
    if (!x)
      return false;
    x->stop_reading();
    return true;
  }

  /// Checks whether `hdl` is assigned to broker.
  template <class Handle>
  bool valid(Handle hdl) {
    return get_map(hdl).count(hdl) > 0;
  }

  /// @cond PRIVATE
  template <class Handle>
  void erase(Handle hdl) {
    auto& elements = get_map(hdl);
    auto i = elements.find(hdl);
    if (i != elements.end())
      elements.erase(i);
  }
  /// @endcond

  // -- overridden observers of abstract_actor ---------------------------------

  const char* name() const override;

  // -- overridden observers of resumable --------------------------------------

  subtype_t subtype() const override;

  // -- observers --------------------------------------------------------------

  /// Returns the number of open connections.
  inline size_t num_connections() const {
    return scribes_.size();
  }

  /// Returns all handles of all `scribe` instances attached to this broker.
  std::vector<connection_handle> connections() const;

  /// Returns the `multiplexer` running this broker.
  network::multiplexer& backend();

protected:
  void init_broker();

  explicit abstract_broker(actor_config& cfg);

  using doorman_map = std::unordered_map<accept_handle, intrusive_ptr<doorman>>;

  using scribe_map = std::unordered_map<connection_handle,
                                        intrusive_ptr<scribe>>;

  /// @cond PRIVATE

  // meta programming utility
  inline doorman_map& get_map(accept_handle) {
    return doormen_;
  }

  // meta programming utility
  inline scribe_map& get_map(connection_handle) {
    return scribes_;
  }

  // meta programming utility (not implemented)
  static intrusive_ptr<doorman> ptr_of(accept_handle);

  // meta programming utility (not implemented)
  static intrusive_ptr<scribe> ptr_of(connection_handle);

  /// @endcond

  /// Returns a `scribe` or `doorman` identified by `hdl`.
  template <class Handle>
  auto by_id(Handle hdl) -> optional<decltype(*ptr_of(hdl))> {
    auto& elements = get_map(hdl);
    auto i = elements.find(hdl);
    if (i == elements.end())
      return none;
    return *(i->second);
  }

  /// Returns an intrusive pointer to a `scribe` or `doorman`
  /// identified by `hdl` and remove it from this broker.
  template <class Handle>
  auto take(Handle hdl) -> decltype(ptr_of(hdl)) {
    using std::swap;
    auto& elements = get_map(hdl);
    decltype(ptr_of(hdl)) result;
    auto i = elements.find(hdl);
    if (i == elements.end())
      return nullptr;
    swap(result, i->second);
    elements.erase(i);
    return result;
  }

private:
  inline void launch_servant(scribe_ptr&) {
    // nop
  }

  void launch_servant(doorman_ptr& ptr);

  template <class T>
  typename T::handle_type add_servant(intrusive_ptr<T>&& ptr) {
    CAF_ASSERT(ptr != nullptr);
    CAF_ASSERT(ptr->parent() == nullptr);
    ptr->set_parent(this);
    auto hdl = ptr->hdl();
    launch_servant(ptr);
    get_map(hdl).emplace(hdl, std::move(ptr));
    return hdl;
  }

  template <class T>
  void move_servant(intrusive_ptr<T>&& ptr) {
    CAF_ASSERT(ptr != nullptr);
    CAF_ASSERT(ptr->parent() != nullptr && ptr->parent() != this);
    ptr->set_parent(this);
    CAF_ASSERT(ptr->parent() == this);
    auto hdl = ptr->hdl();
    get_map(hdl).emplace(hdl, std::move(ptr));
  }

  scribe_map scribes_;
  doorman_map doormen_;
  detail::intrusive_partitioned_list<mailbox_element, detail::disposer> cache_;
  std::vector<char> dummy_wr_buf_;
};

} // namespace io
} // namespace caf

#endif // CAF_IO_ABSTRACT_BROKER_HPP
