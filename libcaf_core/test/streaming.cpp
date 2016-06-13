/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2016                                                  *
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

#include <string>
#include <fstream>
#include <iostream>

#include "caf/config.hpp"

#define CAF_SUITE streaming
#include "caf/test/unit_test.hpp"

#include "caf/all.hpp"

using std::cout;
using std::endl;
using std::string;


namespace {

using namespace caf;

template <class F>
using signature_t = typename detail::get_callable_trait<F>::fun_sig;

struct stream_id {
  strong_actor_ptr origin;
  uint64_t nr;
};

bool operator==(const stream_id& x, const stream_id& y) {
  return x.origin == y.origin && x.nr == y.nr;
}

bool operator<(const stream_id& x, const stream_id& y) {
  return x.origin == y.origin ? x.nr < y.nr : x.origin < y.origin;
}

} // namespace <anonymous>

namespace std {
template <>
struct hash<stream_id> {
  size_t operator()(const stream_id& x) const {
    auto tmp = reinterpret_cast<ptrdiff_t>(x.origin.get())
               ^ static_cast<ptrdiff_t>(x.nr);
    return tmp;
  }
};
} // namespace std

namespace {

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_id& x) {
  return f(meta::type_name("stream_id"), x.origin, x.nr);
}

enum class demand_signaling_policy {
  credit,
  rate
};

std::string to_string(demand_signaling_policy x) {
  return x == demand_signaling_policy::credit ? "credit_based"
                                              : "rate_based";
}

enum class stream_priority {
  low,
  normal,
  high
};

std::string to_string(stream_priority x) {
  switch (x) {
    default: return "invalid";
    case stream_priority::low: return "low";
    case stream_priority::normal: return "normal";
    case stream_priority::high: return "high";
  }
}

/// A stream consumer handles upstream input.
class abstract_consumer : public ref_counted {
public:
  virtual ~abstract_consumer() {}

  /// Called after consuming the final element of a stream.
  virtual void on_complete() {}

protected:
  /// Stores an item in the output buffer, potentially sending it
  /// immediately if sufficient credit is available.
  virtual void push_message(message) = 0;
};

/// 
template <class T>
class consumer : public abstract_consumer {
public:
  virtual void push(T) = 0;

protected:
  void push_message(message x) override final {
    if (x.match_elements<T>())
      push(std::move(x.get_mutable_as<T>(0)));
    else
      CAF_LOG_ERROR("unexpected message in consumer");
  }
};

template <class T>
using consumer_ptr = intrusive_ptr<consumer<T>>;

/// A joint that grants write access to a buffered generator.
template <class T>
class joint : public consumer<T> {
public:
  /// Close this joint. Closing the last joint also closes the parent.
  virtual void close() = 0;

  /// Returns the currently available credit, i.e., work items that
  /// can be sent immediately without buffering. A negative credit
  /// means that the actor has buffered items but not enough credit
  /// to send them.
  virtual int credit() = 0;
};

/// A smart pointer to a `joint`.
template <class T>
using joint_ptr = intrusive_ptr<T>;

/// ```
/// abstract_consumer
/// | * internal interface, consumes a stream of `message` objects
/// | * member functions:
/// |   - push_message(): consume next element of a stream batch
/// |   - demand(): query capacity (signalized upstream)
/// +- consumer<T>
/// |  | * consumes a stream of `T` objects
/// |  | * member functions:
/// |  |   - push(T): consumes next element of a stream batch in its proper type
/// |  +- joint<T>
/// |  |  | * a connection to a buffered generator for pushing elements into a
/// |  |  |   stream, i.e., a joint turns a buffered generator into a consumer
/// |  |  | * member functions:
/// |  |  |   - close(): closes this joint; generators close if no joint is left
/// |  |  |   - credit(): query available downstream capacity
/// |  |  |   - push(T): appends a new tuple to the generator's buffer
/// |  +- y_connector<T>
/// |  |  | * connects to two or more downstream consumer (joints)
///
/// abstract_generator
/// | * internal interface, generates a stream of `message` objects
/// | * member functions:
/// |   - closed(): signalize CAF the stream reached its end
/// |   - make_batch(): get a full batch of stream items
/// |   - credit(): query available downstream capacity
/// +- generator<T>
/// |  | * generic, user-facing interface
/// |  | * member functions:
/// |  |   - pull(): get next item for the stream batch in its proper type
/// |  +- buffered_generator
/// |  |  * full control over emitted tuples via joints
/// |  |  * member functions:
/// |  |    - make_joint(): returns a new joint connected to this generator
/// |  +- transforming_generator:
/// |  |  * emits tuples based on user-generated tranformation
/// ```
///
/// Use cases:
/// - Simple Pipeline: A -> B -> C
///   + B consumes tuples from A
///   + B generates tuples for C
///   + Example: Text processing
/// - Merging: (A1 || A2) -> B -> C
///   + B consumes tuples from A1 and A2 (independent streams)
///   + B generates tuples for C
///   + Example: Load balancing on the edge (e.g. A1 and A2 model HTTP clients),
///     where A1 and A2 drop incoming requests when running out of credit
///   + B splits its capacity between A1 and A2
/// - Splitting: A -> B -> (C1 || C2)
///   + B consumes tuples from A
///   + B generates tuples for C1 and C2 (independent streams)
///   + Example: Load balancing of workers; B dispatches tuples to either C1
///     or C2 depending on available capacity
///   + B joins capacity of C1 and C2, announcing merged demand to A
///
/// # Simple Pipeline
///
/// A uses a generator for emitting tuples downstream. B uses a
/// buffered_generator with one joint. Finally, C uses a consumer,
/// reducing the entire stream into a single result (possibly void).
///
/// # Merging
///
/// B uses a buffered_generator for emitting tuples downstream. This
/// generator has two joints that consume tuples from A1 and A2 by
/// writing into the buffer.
///
/// # Splitting
///
/// B uses a y_connector multplexing between two joints
/// (connected to C1 and C2).
class abstract_generator : public ref_counted {
public:
  abstract_generator() : credit_(0) {
    // nop
  }
  virtual ~abstract_generator() {}
  virtual bool closed() const = 0;
  virtual std::pair<int32_t, message> make_batch(int32_t max_size) = 0;
  inline int credit() const {
    return credit_;
  }

  int inc_credit(int amount = 1) {
    return credit_ += amount;
  }

  int dec_credit(int amount = 1) {
    CAF_ASSERT(amount >= credit_);
    return credit_ -= amount;
  }

private:
  int credit_;
};

using abstract_generator_ptr = intrusive_ptr<abstract_generator>;

template <class T>
class generator : public abstract_generator {
public:
  virtual optional<T> pull() = 0;
  std::pair<int32_t, message> make_batch(int32_t max_size) {
    std::vector<T> xs;
    for (int32_t i = 0; i < max_size; ++i) {
      auto x = pull();
      if (x)
        xs.push_back(std::move(*x));
      else
        break;
    }
    if (!xs.empty()) {
      auto used_credit = static_cast<int32_t>(xs.size());
      return {used_credit, make_message(std::move(xs))};
    }
    return {0, message{}};
  }
};

/// A smart pointer to a generator.
template <class T>
using generator_ptr = intrusive_ptr<generator<T>>;

template <class T>
class buffered_generator : public generator<T> {
public:
  class joint_impl;

  friend class joint_impl;

  buffered_generator() : open_joints_(0) {
    // nop
  }

  joint_ptr<T> make_joint() {
    return new joint_impl(this);
  }

  bool closed() const override {
    return open_joints_ == 0;
  }

  optional<T> pull() override {
    optional<T> result;
    if (!buf_.empty()) {
      result = std::move(buf_.front());
      buf_.pop_front();
    }
    return result;
  }

  class joint_impl final : public joint<T> {
  public:
    using parent_ptr = intrusive_ptr<buffered_generator>;

    joint_impl(parent_ptr parent) : parent_(std::move(parent)) {
      // nop
    }

    void close() override {
      parent_->open_joints_ -= 1;
    }

    int credit() override {
      return parent_->credit()
             - static_cast<int>(parent_->buf_.size());
    }

    void push(T x) override {
      parent_->buf_.emplace_back(std::move(x));
    }

  private:
    parent_ptr parent_;
  };

private:
  size_t open_joints_;
  std::deque<T> buf_;
};

template <class T>
using buffered_generator_ptr = intrusive_ptr<buffered_generator<T>>;

template <class In, class Out>
using stage = std::pair<consumer_ptr<In>, generator_ptr<Out>>;

using abstract_consumer_ptr = intrusive_ptr<abstract_consumer>;

template <class Out>
using half_erased_stage = std::pair<abstract_consumer_ptr, generator_ptr<Out>>;

using abstract_stage = std::pair<intrusive_ptr<abstract_consumer>,
                                 intrusive_ptr<abstract_generator>>;

template <class T>
class stream {
public:
  stream() = default;
  stream(stream&&) = default;
  stream(const stream&) = default;
  stream& operator=(stream&&) = default;
  stream& operator=(const stream&) = default;

  stream(stream_id sid) : id_(std::move(sid)) {
    // nop
  }

  const stream_id& id() const {
    return id_;
  }

  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, stream& x) {
    return f(x.id_);
  }

private:
  stream_id id_;
};

template <class Inspector, class T>
typename Inspector::result_type inspect(Inspector&, stream<T>&) {
  // nop
}

struct stream_msg {
  /// Initiates a stream handshake.
  struct open {
    /// A type-erased stream<T> object for picking the correct message
    /// handler of the receiving actor.
    message token;
    /// Configures the priority for stream elements.
    stream_priority priority;
    /// Configures the demand policy, e.g., rate-based or credit-based.
    demand_signaling_policy policy;
  };

  /// Finalizes a stream handshake and signalizes initial demand.
  struct ok {
    /// Sets either an initial send rate or grants credit.
    int32_t initial_demand;
  };

  /// Transmits work items.
  struct batch {
    /// Size of the type-erased vector<T> (released credit).
    int32_t xs_size;
    /// A type-erased vector<T> containing the elements of the batch.
    message xs;
  };

  /// Signalizes demand from a sink to its source. The receiver interprets
  /// the value according to the stream demand signaling policy for
  /// the stream ID.
  struct demand {
    int32_t value;
  };

  /// Closes a stream.
  struct close {
    // tag type
  };

  stream_id sid;
  variant<open, ok, batch, demand, close> content;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::open& x) {
  return f(meta::type_name("open"), x.token, x.priority, x.policy);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::ok& x) {
  return f(meta::type_name("ok"), x.initial_demand);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::batch& x) {
  return f(meta::type_name("batch"), meta::omittable(), x.xs_size, x.xs);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::demand& x) {
  return f(meta::type_name("demand"), x.value);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::close&) {
  return f(meta::type_name("close"));
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg& x) {
  return f(meta::type_name("stream_msg"), x.sid, x.content);
}

template <class T>
stream_msg make_stream_open(stream<T> token,
                            stream_priority pr = stream_priority::normal,
                            demand_signaling_policy po = demand_signaling_policy::credit) {
  // make_message rejects stream<T>, because stream<T> has special meaning
  // in CAF. However, we still need the token in order to dispatch to the
  // user-defined message handler. This code bypasses make_message
  // for this purpose.
  using storage = detail::tuple_vals<stream<T>>;
  auto ptr = make_counted<storage>(token);
  message msg{detail::message_data::cow_ptr{std::move(ptr)}};
  return {std::move(token.id()), stream_msg::open{std::move(msg), pr, po}};
}

inline stream_msg make_stream_ok(stream_id sid, int32_t initial_demand) {
  return {std::move(sid), stream_msg::ok{initial_demand}};
}

inline stream_msg make_stream_batch(stream_id sid, int32_t xs_size, message xs) {
  return {std::move(sid), stream_msg::batch{xs_size, std::move(xs)}};
}

inline stream_msg make_stream_demand(stream_id sid, int32_t value) {
  return {std::move(sid), stream_msg::demand{value}};
}

inline stream_msg make_stream_close(stream_id sid) {
  return {std::move(sid), stream_msg::close{}};
}



} // namespace <anonymous>

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<int>)

namespace {

#define debug(x)                                                               \
  aout(self) << "[id: " << self->id() << " line " << __LINE__ << "] "          \
             << self->current_mailbox_element()->content() << std::endl

#if 0

template <class f, class g>
constexpr bool HasSignature() {
  return std::is_same<f, signature_t<g>>::value;
}

template <class State, class T, class Init, class Pull, class Closed>
generator_ptr<T> make_generator(Init init, Pull pull, Closed closed) {
  static_assert(HasSignature<void (State&), Init>(),
                "first argument must have have signature 'void (state&)'");
  static_assert(HasSignature<optional<T> (State&), Pull>(),
                "second argument must have signature 'optional<T> (state&)'");
  static_assert(HasSignature<bool (const State&), Closed>(),
                "third argument must have signature 'bool (state&)'");
  class impl final : public generator<T> {
  public:
    impl(Pull f1, Closed f2) : pull_(f1), closed_(f2) {
      // nop
    }
    optional<T> pull() override {
      return pull_(state_);
    }
    bool closed() const override {
      return closed_(state_);
    }
    State& state() {
      return state_;
    }
  private:
    State state_;
    Pull pull_;
    Closed closed_;
  };
  auto result = make_counted<impl>(std::move(pull), std::move(closed));
  init(result->state());
  return result;
}

template <class State, class In, class Out,
          class Init, class Transform, class OnComplete>
stage<In, Out> make_stage(Init init, Transform transform, OnComplete on_complete) {
  class consumer_impl final : public consumer<In> {
  public:
    using fun = Transform;
    using parent_ptr = buffered_generator_ptr<Out>;
    consumer_impl(parent_ptr parent, Transform f0, OnComplete f1)
      : joint_(std::move(parent)),
        f_(std::move(f0)),
        on_complete_(std::move(f1)) {
      // nop
    }
    void push(In x) override {
      f_(state_, joint_, std::move(x));
    }
    void on_complete() override {
      // nop
    }
    State& state() {
      return state_;
    }
  private:
    State state_;
    typename buffered_generator<Out>::joint_impl joint_;
    fun f_;
    OnComplete on_complete_;
  };
  auto g = make_counted<buffered_generator<Out>>();
  auto s = make_counted<consumer_impl>(g, std::move(transform),
                                       std::move(on_complete));
  init(s->state());
  return std::make_pair(std::move(s), std::move(g));
}

template <class State, class In, class Out, class Transform>
stage<In, Out> make_stage(Transform f) {
  return make_stage<State, In, Out>([](State&) { }, f, [](State&) { });
}

behavior file_reader() {
  return {
    [=](const std::string& path) -> stream<int> {
      printf("%s\n", "file_reader");
      return make_generator<std::ifstream, int>(
        // initialize state
        [&](std::ifstream& f) {
          f.open(path);
        },
        // get next element
        [](std::ifstream& f) -> optional<int> {
          int res;
          if (f >> res)
            return res;
          return none;
        },
        // check whether we reached the end
        [](const std::ifstream& f) {
          return !f;
        }
      );
    }
  };
}

behavior filter_odd_nums() {
  return {
    [=](stream<int>) -> stream<int> {
      printf("%s\n", "filter_odd_nums");
      return make_stage<unit_t, int, int>(
        // process element
        [](unit_t&, joint<int>& out, int x) {
          if (x % 2 == 0)
            out.push(x);
        }
      );
    }
  };
}

behavior drop_sink() {
  return {
    [=](stream<int>) {

    }
  };
}

/*
behavior file_writer(const std::string& path) {
  return {
    [=](stream<int>) -> void {
      return make_sink<std::ofstream, int>(
        // initialize state
        [=](std::ofstream& f) {
          f.open(path);
        },
        // process element
        [](std::ofstream& f, int x) {
          f << x << '\n';
        },
        // called after receiving the last element of the stream
        [](std::ofstream&) {
          // nop
        }
      );
    }
  };
}
*/
struct downstream {
public:
  downstream(stream_id sid, strong_actor_ptr sink, abstract_generator_ptr gptr)
      : sid_(std::move(sid)),
        sink_(std::move(sink)),
        credit_(0),
        gptr_(std::move(gptr)) {
    CAF_ASSERT(sink_ != nullptr);
  }

  const stream_id& id() const {
    return sid_;
  }

  const strong_actor_ptr& sink() const {
    return sink_;
  }

  int32_t credit() const {
    return credit_;
  }

  void inc_credit(int32_t x) {
    credit_ += x;
  }

  void dec_credit(int32_t x) {
    credit_ -= x;
  }

  bool closed() const {
    return gptr_->closed();
  }

  /// Send a batch of items to the sink.
  void send_batch(scheduled_actor* self) {
    CAF_ASSERT(self != nullptr);
    CAF_ASSERT(sink_ != nullptr);
    CAF_ASSERT(gptr_ != nullptr);
    auto s = credit_;
    aout(self) << "[id: " << self->id() << "] available credit: "
               << s << std::endl;
    if (s > 0) {
      auto res = gptr_->make_batch(s);
      credit_ -= res.first;
      aout(self) << "[id: " << self->id() << "] transmit batch: "
                 << res.second << std::endl;
      if (!res.second.empty())
        sink_->enqueue(self->ctrl(), message_id::make(),
                       make_message(make_stream_batch(sid_, res.first, 
                                                      std::move(res.second))),
                       self->context());
      if (gptr_->closed())
        sink_->enqueue(self->ctrl(), message_id::make(),
                       make_message(make_stream_close(sid_)),
                       self->context());
    }
  }

private:
  /// Stream ID as seen by the downstream actor.
  stream_id sid_;
  /// Handle to the successor stage.
  strong_actor_ptr sink_;
  /// Currently available credit.
  int32_t credit_;
  /// Generator for creating batches.
  abstract_generator_ptr gptr_;
};

class upstream {
public:
  upstream(stream_id sid, strong_actor_ptr predecessor)
      : sid_(std::move(sid)),
        source_(std::move(predecessor)) {
    CAF_ASSERT(source_ != nullptr);
    CAF_ASSERT(max_credit_ > 0);
  }

  upstream(upstream&&) = default;
  upstream(const upstream&) = default;
  upstream& operator=(upstream&&) = default;
  upstream& operator=(const upstream&) = default;

  const stream_id& id() const {
    return sid_;
  }

  const strong_actor_ptr& source() const {
    return source_;
  }

  int32_t low_watermark() const {
    return low_watermark_;
  }

  int32_t assigned_credit() const {
    return assigned_credit_;
  }

  int32_t max_credit() const {
    return max_credit_;
  }

  /// Send demand to the source.
  void send_demand(scheduled_actor* self, int32_t released_credit) {
    assigned_credit_ -= released_credit;
    if (assigned_credit_ < low_watermark_) {
      assigned_credit_ += 10;
      source_->enqueue(self->ctrl(), message_id::make(),
                       make_message(make_stream_demand(sid_, 10)),
                       self->context());
    }
  }

private:
  /// Stream ID as seen by the upstream actor.
  stream_id sid_;
  /// Handle to the predecessor stage.
  strong_actor_ptr source_;
  /// Configures threshold for sending new demand.
  int32_t low_watermark_;
  /// Amount of open credit assigned to the source.
  int32_t assigned_credit_;
  /// Maximum amount of credit allowed for the source.
  int32_t max_credit_;
};

struct stage_data : ref_counted {
  std::vector<upstream> upstreams;
  std::vector<downstream> downstreams;
};

using stage_data_ptr = intrusive_ptr<stage_data>;

struct stream_state {
  std::unordered_map<stream_id, stage_data_ptr> data;
};

struct stream_stage_visitor {
public:
  using result_type = void;

  stream_stage_visitor(scheduled_actor* self, stream_state& st, stream_id& sid)
      : self_(self),
        st_(st),
        sid_(sid) {
    // nop
  }
  void operator()(stream_msg::open& x) {
    auto predecessor = self_->current_sender();
    if (!predecessor)
      return;
    auto cme = self_->current_mailbox_element();
    if (!cme) {
      CAF_LOG_ERROR("stream_msg: current_mailbox_element == nullptr");
      return;
    }
    // little helper for sending async messages
    auto asend = [&](strong_actor_ptr& dest, message msg) {
      dest->enqueue(self_->ctrl(), message_id::make(),
                    std::move(msg), self_->context());
    };
    auto i = st_.data.find(sid_);
    if (i == st_.data.end()) {
      i = st_.data.emplace(sid_, make_counted<stage_data>()).first;
    }
    auto& bs = self_->bhvr_stack();
    if (bs.empty()) {
      CAF_LOG_ERROR("cannot open stream in actor without behavior");
      asend(predecessor, make_message(sec::unexpected_message));
      return;
    }
    auto bhvr = self_->bhvr_stack().back();
    auto res = bhvr(x.token);
    if (!res) {
      CAF_LOG_ERROR("Stream handshake failed: actor did not respond to token:"
                    << CAF_ARG(x.token));
      asend(predecessor, make_message(sec::unexpected_message));
      return;
    }
    if (!res->match_elements<stream<int>>()) {
      CAF_LOG_ERROR("expected a stream<int> from handler, got:" << CAF_ARG(res));
      return;
    }
    auto ptr = get<generator_ptr<int>>(&res->get_mutable_as<stream<int>>(0).value());
    if (!ptr) {
      CAF_LOG_ERROR("stream not initialized using a generator");
      return;
    }
    // check whether we are a stage in a longer pipeline
    if (!cme->stages.empty()) {
      aout(self_) << "[id: " << self_->id() << "]: " << "send handshake to next stage "<< endl;
      auto next = cme->stages.back();
      cme->stages.pop_back();
      asend(next, make_message(make_stream_open(stream<int>{sid_})));
      i->second->downstreams.emplace_back(sid_, std::move(next), std::move(*ptr));

    }
    //i->second.downstreams.emplace_back(sid_, predecessor, 7, 10, 25);
    i->second->upstreams.emplace_back(sid_, predecessor);
    asend(predecessor, make_message(make_stream_ok(std::move(sid_), 5)));
  }

  void operator()(stream_msg::ok& x) {
    auto successor = self_->current_sender();
    if (!successor)
      return;
    auto i = st_.data.find(sid_);
    if (i != st_.data.end()) {
      auto& ds = i->second->downstreams.back();
      ds.inc_credit(x.initial_demand);
      ds.send_batch(self_);
      // TODO: properly support multiple downstreams, i.e.,
      // don't drop stream on first close/error
      if (ds.closed())
        st_.data.erase(i);
    }
  }

  void operator()(stream_msg::batch& x) {
    // TODO: support multiple upstreams
    auto i = st_.data.find(sid_);
    if (i != st_.data.end())
      i->second->upstreams.back().send_demand(self_, x.xs_size);
  }

  void operator()(stream_msg::demand& x) {
    auto i = st_.data.find(sid_);
    if (i != st_.data.end()) {
      // TODO: support multiple downstreams
      auto& ds = i->second->downstreams.back();
      ds.inc_credit(x.value);
      ds.send_batch(self_);
      if (ds.closed())
        st_.data.erase(i);
    }
  }

  void operator()(stream_msg::close&) {
    // TODO: support multiple upstreams
    auto i = st_.data.find(sid_);
    if (i != st_.data.end())
      i->second->upstreams.clear();
  }

private:
  scheduled_actor* self_;
  stream_state& st_;
  stream_id& sid_;
};

behavior streamer(stateful_actor<stream_state>* self, behavior bhvr) {
  message_handler tmp {
    [=](std::string& fname) mutable -> delegated<void> {
      debug();
      stream_id sid{self->ctrl(), 0};
      auto& cme = *self->current_mailbox_element();
      if (cme.stages.empty()) {
        // we are the sink for this stream (TODO: implement)
        return {};
      }
      auto new_sink = cme.stages.back();
      CAF_ASSERT(new_sink);
      self->send(actor_cast<actor>(new_sink), make_stream_open(stream<int>{sid}));
      auto& st = self->state;
      auto fname_msg = make_message(std::move(fname));
      auto res = bhvr(fname_msg);
      if (!res) {
        CAF_LOG_ERROR("failed to init stream with message");
        return {};
      }
      if (!res->match_elements<stream<int>>()) {
        CAF_LOG_ERROR("expected a stream<int> from handler, got: " << CAF_ARG(res));
        return {};
      }
      auto ptr = get<generator_ptr<int>>(&res->get_mutable_as<stream<int>>(0).value());
      if (!ptr) {
        CAF_LOG_ERROR("stream not initialized using a generator");
        return {};
      }
      auto i = st.data.emplace(sid, make_counted<stage_data>()).first;
      i->second->downstreams.emplace_back(sid, actor_cast<strong_actor_ptr>(new_sink),
                                          std::move(*ptr));
      return {};
    },
    [=](stream_msg& x) {
      debug();
      stream_stage_visitor v{self, self->state, x.sid};
      apply_visitor(v, x.content);
    }
  };
  return tmp.or_else(bhvr);
}

#endif // 0

namespace new_design {

/// Manages a stream.
class stream_handler : public ref_counted {
public:
  /// Add a new downstream actor to the stream.
  virtual error add_downstream(stream_id& id, strong_actor_ptr& sink) = 0;

  /// Add credit to a downstream.
  virtual error add_credit(stream_id& id, strong_actor_ptr& sink, int32_t amount) = 0;

  /// Sends data downstream if possible. If `cause` is not `nullptr` then it
  /// is interpreted as the last downstream actor that signaled demand.
  virtual error trigger_send(strong_actor_ptr& cause) = 0;

  /// Add a new upstream actor to the stream.
  virtual error add_upstream(stream_id& id, strong_actor_ptr& source) = 0;

  /// Handles data from an upstream actor.
  virtual error handle_data(stream_id& id, strong_actor_ptr& source, message& batch) = 0;

  /// Closes an upstream.
  virtual error close_upstream(stream_id& id, strong_actor_ptr& source) = 0;

  /// Sends demand upstream if possible. If `cause` is not `nullptr` then it
  /// is interpreted as the last upstream actor that send data.
  virtual error trigger_demand(strong_actor_ptr& cause) = 0;
};

/// Smart pointer to a stream handler.
/// @relates stream_handler
using stream_handler_ptr = intrusive_ptr<stream_handler>;

/// Mixin for streams without upstreams.
template <class Base, class Subtype>
class no_upstreams : public Base {
public:
  error add_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::cannot_add_upstream;
  }

  error handle_data(stream_id&, strong_actor_ptr&, message&) final {
    CAF_LOG_ERROR("Cannot handle upstream data in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error close_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error trigger_demand(strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot trigger demand in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }
};

/// Mixin for streams without downstreams.
template <class Base, class Subtype>
class no_downstreams : public Base {
public:
  error add_downstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add downstream to a stream marked as no-downstreams");
    return sec::cannot_add_downstream;
  }

  error add_credit(stream_id&, strong_actor_ptr&, int32_t) final {
    CAF_LOG_ERROR("Cannot handle downstream demand in a stream marked as no-downstreams");
    return sec::invalid_downstream;
  }

  error trigger_send(strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot send downstream in a stream marked as no-downstream");
    return sec::invalid_downstream;
  }
};

struct sink {
  int32_t credit;
  strong_actor_ptr ptr;
};

struct source {
  int32_t assigned_credit;
  strong_actor_ptr ptr;
};


inline bool credit_cmp(const std::pair<const stream_id, sink>& x,
                       const std::pair<const stream_id, sink>& y) noexcept {
  return x.second.credit < y.second.credit;
}


template <class T>
class downstream {
public:
  using sinks_map = std::unordered_map<stream_id, sink>;

  using value_type = typename sinks_map::value_type;

  downstream(local_actor* self) : self_(self) {
    // nop
  }

  template <class... Ts>
  void push(Ts&&... xs) {
    buf_.emplace_back(std::forward<Ts>(xs)...);
  }

  /// Returns the total available credit for all sinks in O(n).
  int32_t total_credit() const {
    auto add = [](int32_t x, const value_type& y) {
      return x + y.second.credit;
    };
    return std::accumulate(sinks_.begin(), sinks_.end(), int32_t{0}, add);
  }

  /// Returns the minimal credit of all sinks in O(n).
  int32_t min_credit() const {
    auto e = sinks_.end();
    auto i = std::min_element(sinks_.begin(), e, credit_cmp);
    return i == e ? 0 : i->second.credit;
  }

  sinks_map& sinks() {
    return sinks_;
  }

  const sinks_map& sinks() const {
    return sinks_;
  }

  std::vector<T>& buf() {
    return buf_;
  }

  void send_batch(sink& dest, std::vector<T> batch) {
    dest.credit -= batch.size();
  }

private:
  local_actor* self_;
  std::vector<T> buf_;
  sinks_map sinks_;
};

template <class T>
class upstream {
public:
  using sources_map = std::unordered_map<stream_id, source>;

  sources_map& sources() {
    return sources_;
  }

private:
  int32_t unassigned_credit_;
  int32_t max_credit_;
  int32_t low_watermark_;
  std::unordered_map<stream_id, source> sources_;
};

namespace policy {

/// A stream source policy striving to use all available credit
/// without considering ordering or observed downstream behavior.
class bruteforce {
public:
  template <class T>
  void send_batches(downstream<T>& out, strong_actor_ptr&) {
    std::vector<T> buf;
    buf.swap(out.buf());
    for (auto& kvp : out.sinks()) {
      auto credit = kvp.second.credit;
      if (credit > 0) {
        if (static_cast<size_t>(credit) >= buf.size()) {
          out.send_batch(kvp.second, std::move(buf));
          return;
        } else {
          // move excess items from buffer into temporary
          auto i = std::make_move_iterator(buf.begin() + credit);
          auto e = std::make_move_iterator(buf.end());
          std::vector<T> leftover{i, e};
          // resize and send buf
          buf.resize(static_cast<size_t>(credit));
          out.send_batch(kvp.second, std::move(buf));
          // swap with temporary for next iteration
          swap(buf, leftover);
        }
      }
    }
  }

  template <class T>
  size_t desired_buffer_size(downstream<T>& out) {
    return out.total_credit();
  }
};

} // namespace policy

template <class T, class Producer, class Policy>
class stream_source : public extend<stream_handler>::with<no_upstreams> {
public:
  template <class... Ts>
  stream_source(local_actor* self, Policy policy, Ts&&... xs)
      : producer_(std::forward<Ts>(xs)...),
        downstream_(self),
        policy_(std::move(policy)) {
    // nop
  }

  error add_downstream(stream_id& sid, strong_actor_ptr& ptr) final {
    if (!ptr)
      return sec::invalid_argument;
    auto i = downstream_.sinks().find(sid);
    if (i != downstream_.sinks().end()) {
      downstream_.sinks().emplace(sid, sink{0, ptr});
      return none;
    }
    return sec::downstream_already_exists;
  }

  error add_credit(stream_id& sid, strong_actor_ptr& ptr, int32_t value) final {
    auto i = downstream_.sinks().find(sid);
    if (i != downstream_.sinks().end()) {
      i->second.credit += value;
      if (!producer_.closed()) {
        auto current_size = downstream_.buf().size();
        auto size_hint = policy_.desired_buffer_size(downstream_);
        if (current_size < size_hint)
          producer_(downstream_, size_hint - current_size);
      }
      return trigger_send(ptr);
    }
    return sec::invalid_downstream;
  }

  error trigger_send(strong_actor_ptr& cause) final {
    if (!downstream_.buf().empty())
      policy_.send_batches(downstream_, cause);
    return none;
  }

  Producer& producer() {
    return producer_;
  }

private:
  Producer producer_;
  downstream<T> downstream_;
  Policy policy_;
};

template <class T, class Consumer, class Policy>
class stream_sink : public extend<stream_handler>::with<no_downstreams> {
public:
  stream_sink(Consumer consumer, Policy policy)
      : consumer_(std::move(consumer)),
        policy_(std::move(policy)) {
    // nop
  }

  error add_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::cannot_add_upstream;
  }

  error handle_data(stream_id&, strong_actor_ptr&, message&) final {
    CAF_LOG_ERROR("Cannot handle upstream data in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error close_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error trigger_demand(strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot trigger demand in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

private:
  Consumer consumer_;
  upstream<T> upstream_;
  Policy policy_;
};

template <class State, class In, class Out, class Processor>
class stream_stage : public stream_handler {
public:
  stream_stage(Processor processor) : processor_(std::move(processor)) {
    // nop
  }

private:
  Processor processor_;
  upstream<In> upstream_;
  downstream<Out> downstream_;
};

class streamer : public event_based_actor {
public:
  streamer(actor_config& cfg) : event_based_actor(cfg), next_stream_nr(0) {
    // nop
  }

  template <class T>
  stream<T> make_source() {
    return make_stream_id();
  }

  template <class T>
  delegated<T> make_sink(const stream<T>&) {
    return {};
  }

  template <class T>
  stream<T> make_stage(const stream<T>& x) {
    return x.id();
  }

  stream_id make_stream_id() {
    return {ctrl(), next_stream_nr++};
  }

private:
  uint64_t next_stream_nr;
  std::unordered_map<stream_id, stream_handler_ptr> streams_;
};

} // namespace new_design

template <class F>
struct stream_source_trait;

template <class State, class T>
struct stream_source_trait<int32_t (State&, new_design::downstream<T>&, int32_t)> {
  using output = T;
  using state = State;
};

template <class F>
using stream_source_trait_t = stream_source_trait<typename detail::get_callable_trait<F>::fun_sig>;

template <class F>
struct stream_stage_trait;

template <class State, class In, class Out>
struct stream_stage_trait<void (State&, new_design::downstream<Out>&, In)> {
  using state = State;
  using input = In;
  using output = Out;
};

template <class F>
using stream_stage_trait_t = stream_stage_trait<typename detail::get_callable_trait<F>::fun_sig>;

template <class State, class F, class Pred>
class data_source {
public:
  using value_type = typename stream_source_trait_t<F>::output;

  data_source(F fun, Pred pred) : fun_(std::move(fun)), pred_(std::move(pred)) {
    // nop
  }

  int32_t operator()(new_design::downstream<value_type>& out, int32_t num) {
    return fun_(state_, out, num);
  }

  bool closed() const {
    return pred_(state_);
  }

  State& state() {
    return state_;
  }

private:
  State state_;
  F fun_;
  Pred pred_;
};

class mock_actor : public extend<local_actor, mock_actor>::with<mixin::sender> {
public:
  using signatures = none_t;
  using behavior_type = behavior;

  mock_actor(actor_config& cfg) : extended_base(cfg) {
    default_.assign(
      [=](stream_msg&) {
        auto self = this;
        debug();
      }
    );
    bhvr_ = default_;
  }

  void launch(execution_unit*, bool, bool) {
    // nop
  }

  void enqueue(mailbox_element_ptr what, execution_unit*) {
    mailbox_.enqueue(what.release());
  }

  optional<message> pop() {
    mailbox_element_ptr ptr{mailbox().try_pop()};
    if (ptr)
      return ptr->move_content_to_message();
    return none;
  }

  actor hdl() {
    return actor_cast<actor>(ctrl());
  }

  optional<type_erased_tuple&> peek() {
    auto ptr = mailbox().peek();
    if (ptr)
      return ptr->content();
    return none;
  }

  bool activate() {
    auto ptr = next_message();
    if (!ptr)
      return false;
    current_element_ = ptr.get();
    auto self = this;
    debug();
    bhvr_(ptr->content());
    return true;
  }

  void set_behavior(behavior x) {
    bhvr_ = default_.or_else(x);
  }

  template <class Init, class Getter, class ClosedPredicate>
  stream<typename stream_source_trait_t<Getter>::output>
  add_source(Init init, Getter getter, ClosedPredicate pred) {
    printf("%s line %d\n", __FILE__, __LINE__);
    using type = typename stream_source_trait_t<Getter>::output;
    using state_type = typename stream_source_trait_t<Getter>::state;
    static_assert(std::is_same<
                    void (state_type&),
                    typename detail::get_callable_trait<Init>::fun_sig
                  >::value,
                  "Expected signature `void (State&)` for init function");
    static_assert(std::is_same<
                    bool (const state_type&),
                    typename detail::get_callable_trait<ClosedPredicate>::fun_sig
                  >::value,
                  "Expected signature `bool (const State&)` for "
                  "closed_predicate function");
    auto mptr = current_mailbox_element();
    if (!mptr) {
      CAF_LOG_ERROR("add_source called outside of a message handler");
      return stream_id{nullptr, 0};
    }
    auto& stages = mptr->stages;
    if (stages.empty() || !stages.back()) {
      CAF_LOG_ERROR("cannot create a stream data source without next stage");
      auto rp = make_response_promise();
      rp.deliver(sec::no_downstream_stages_defined);
      return stream_id{nullptr, 0};
    }
    stream_id sid{ctrl(), new_request_id(message_priority::normal).integer_value()};
    auto next = std::move(stages.back());
    stages.pop_back();
    stream<type> token{sid};
    next->enqueue(make_mailbox_element(ctrl(), message_id::make(),
                                       std::move(stages),
                                       make_message(make_stream_open(token))),
                  context());
    using source_type = data_source<state_type, Getter, ClosedPredicate>;
    new_design::policy::bruteforce p;
    using impl = new_design::stream_source<type, source_type, new_design::policy::bruteforce>;
    auto ptr = make_counted<impl>(this, p, std::move(getter), std::move(pred));
    init(ptr->producer().state());
    f_ = std::move(ptr);
    return token;
  }

  template <class In, class Init, class Fun, class Cleanup>
  stream<typename stream_stage_trait_t<Fun>::output>
  add_stage(stream<In>& in, Init init, Fun fun, Cleanup cleanup) {
    return std::move(in.id());
  }

  behavior bhvr_;
  message_handler default_;
  new_design::stream_handler_ptr f_;
};

behavior file_reader_(mock_actor* self) {
  using vec = std::vector<int>;
  return {
    [=](const std::string& path) -> stream<int> {
      printf("%s line %d\n", "file_reader", __LINE__);
      return self->add_source(
        // initialize state
        [&](vec& xs) {
          printf("%s line %d\n", "file_reader", __LINE__);
          xs = vec{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
        },
        // get next element
        [](vec& xs, new_design::downstream<int>& out, int32_t num) -> int32_t {
          printf("%s line %d\n", "file_reader", __LINE__);
          auto n = std::min(num, static_cast<int32_t>(xs.size()));
          for (int32_t i = 0; i < n; ++i)
            out.push(xs[i]);
          xs.erase(xs.begin(), xs.begin() + static_cast<size_t>(n));
          return n;
        },
        // check whether we reached the end
        [](const vec& xs) {
          printf("%s line %d\n", "file_reader", __LINE__);
          return xs.empty();
        }
      );
    }
  };
}

behavior filter_(mock_actor* self) {
  return {
    [=](stream<int>& x) -> stream<int> {
      return self->add_stage(
        // input stream
        x,
        // initialize state
        [](unit_t&) {
          // nop
        },
        // processing step
        [](unit_t&, new_design::downstream<int>& out, int x) {
          if (x & 0x01)
            out.push(x);
        },
        // cleanup
        [](unit_t&) {
          // nop
        }
      );
    }
  };
}

void intrusive_ptr_add_ref(mock_actor* ptr) {
  caf::intrusive_ptr_add_ref(ptr->ctrl());
}

void intrusive_ptr_release(mock_actor* ptr) {
  caf::intrusive_ptr_release(ptr->ctrl());
}

intrusive_ptr<mock_actor> make_mock_actor(actor_system& sys) {
  actor_config cfg{sys.dummy_execution_unit()};
  auto ptr = make_actor<mock_actor, strong_actor_ptr>(sys.next_actor_id(),
                                                      sys.node(), &sys, cfg);
  return static_cast<mock_actor*>(ptr->get());
}

/*
void test2(actor_system& sys, const config&) {
  using iface_src = typed_actor<replies_to<std::string>::with<stream<int>>>;
  auto src = sys.spawn(streamer, file_reader());
  auto stg = sys.spawn(streamer, filter_odd_nums());
  auto snk = sys.spawn(streamer, behavior{});
  //anon_send(src, add_atom::value, snk, "test.txt");
  auto f_src = make_function_view(actor_cast<iface_src>(src));
  auto res = f_src("test.txt");
  CAF_CHECK_EQUAL(res, make_error(sec::no_downstream_stages_defined));
  auto pipeline = stg * src;
  anon_send(pipeline, "test.txt");
}

void old_test() {
  auto source = make_mock_actor(sys);
  auto stage = make_mock_actor(sys);
  auto sink = make_mock_actor(sys);
  source->send(stage->hdl(), 123);
  cout << "message in stage? " << to_string(stage->pop()) << endl;
  source->set_behavior(file_reader_(source.get()));
  anon_send(source->hdl(), "test.txt");
  source->activate();
}
*/

struct fixture {
  actor_system_config cfg;
  actor_system sys;
  scoped_actor self;

  fixture() : sys(cfg), self(sys) {
    // nop
  }

  template <class T>
  expected<T> fetch_result() {
    expected<T> result = error{};
    self->receive(
      [&](T& x) {
        result = std::move(x);
      },
      [&](error& x) {
        result = std::move(x);
      }
    );
    return result;
  }
};

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(streaming_tests, fixture)

CAF_TEST(pipeline_test) {
  using iface_src = typed_actor<replies_to<std::string>::with<stream<int>>>;
  auto source = make_mock_actor(sys);
  source->set_behavior(file_reader_(source.get()));
  auto stage = make_mock_actor(sys);
  stage->set_behavior(filter_(stage.get()));
  auto sink = make_mock_actor(sys);
  CAF_MESSAGE("opening streams must fail if no downstream stage exists");
  {
    self->send(source->hdl(), "test.txt");
    source->activate();
    auto res = fetch_result<stream<int>>();
    CAF_CHECK_EQUAL(res, sec::no_downstream_stages_defined);
  }
  CAF_MESSAGE("streams must abort if not reaching a sink");
  {
    auto pipeline = stage->hdl() * source->hdl();
    self->send(pipeline, "test.txt");
    source->activate();
    auto open_msg = stage->peek();
    CAF_REQUIRE(open_msg);
    CAF_REQUIRE(open_msg->match_elements<stream_msg>());
    CAF_REQUIRE(open_msg->get_as<stream_msg>(0).content.is<stream_msg::open>());
    stage->activate();
    /*
    auto res = fetch_result<stream<int>>();
    CAF_CHECK_EQUAL(res, sec::no_downstream_stages_defined);
    */
  }
}

CAF_TEST_FIXTURE_SCOPE_END()
