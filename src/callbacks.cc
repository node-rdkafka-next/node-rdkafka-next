/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <string>
#include <vector>
#include <algorithm>

#include "src/callbacks.h"
#include "src/kafka-consumer.h"

using Napi::Object;
using Napi::Array;
using Napi::Number;

namespace NodeKafka {
namespace Callbacks {

Napi::Array TopicPartitionListToV8Array(Napi::Env env,
  std::vector<event_topic_partition_t> parts) {
  Napi::Array tp_array = Napi::Array::New(env);

  for (size_t i = 0; i < parts.size(); i++) {
    Napi::Object tp_obj = Napi::Object::New(env);
    event_topic_partition_t tp = parts[i];

    tp_obj.Set("topic",
      Napi::String::New(env, tp.topic.c_str()));
    tp_obj.Set("partition",
      Napi::Number::New(env, tp.partition));

    if (tp.offset >= 0) {
      tp_obj.Set("offset",
        Napi::Number::New(env, tp.offset));
    }

    tp_array.Set(i, tp_obj);
  }

  return tp_array;
}

Dispatcher::Dispatcher() {
  async = NULL;
  uv_mutex_init(&async_lock);
}

Dispatcher::~Dispatcher() {
  if (callbacks.size() < 1) return;

  // for (size_t i=0; i < callbacks.size(); i++) {
  //   callbacks[i].Reset();
  // }

  uv_mutex_destroy(&async_lock);
}

// Only run this if we aren't already listening
void Dispatcher::Activate() {
  if (!async) {
    async = new uv_async_t;
    uv_async_init(uv_default_loop(), async, AsyncMessage_);

    async->data = this;
  }
}

// Should be able to run this regardless of whether it is active or not
void Dispatcher::Deactivate() {
  if (async) {
    uv_close(reinterpret_cast<uv_handle_t*>(async), NULL);
    async = NULL;
  }
}

bool Dispatcher::HasCallbacks() {
  return callbacks.size() > 0;
}

void Dispatcher::Execute() {
  if (async) {
    uv_async_send(async);
  }
}

void Dispatcher::Dispatch(const std::vector<napi_value>& args) {
  // This should probably be an array of v8 values
  if (!HasCallbacks()) {
    return;
  }

  for (size_t i=0; i < callbacks.size(); i++) {
    Napi::Function cb =  callbacks[i];
    cb.Call( args);
  }
}

void Dispatcher::AddCallback(const Napi::Function cb) {
  // PersistentCopyableFunction value(func);
  callbacks.push_back(cb);
}

void Dispatcher::RemoveCallback(const Napi::Function cb) {
  for (size_t i=0; i < callbacks.size(); i++) {
    if (callbacks[i] == cb) {
      // callbacks[i].Reset();
      callbacks.erase(callbacks.begin() + i);
      break;
    }
  }
}

event_t::event_t(const RdKafka::Event &event) {
  message = "";
  fac = "";

  type = event.type();

  switch (type = event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      message = RdKafka::err2str(event.err());
    break;
    case RdKafka::Event::EVENT_STATS:
      message = event.str();
    break;
    case RdKafka::Event::EVENT_LOG:
      severity = event.severity();
      fac = event.fac();
      message = event.str();
    break;
    case RdKafka::Event::EVENT_THROTTLE:
      message = RdKafka::err2str(event.err());
      throttle_time = event.throttle_time();
      broker_name = event.broker_name();
      broker_id = static_cast<int>(event.broker_id());
    break;
    default:
      message = event.str();
    break;
  }
}
event_t::~event_t() {}

// Event callback
Event::Event():
  dispatcher() {}

Event::~Event() {}

void Event::event_cb(RdKafka::Event &event) {
  // Second parameter is going to be an object with properties to
  // represent the others.

  if (!dispatcher.HasCallbacks()) {
    return;
  }

  event_t e(event);

  dispatcher.Add(e);
  dispatcher.Execute();
}

EventDispatcher::EventDispatcher() {}
EventDispatcher::~EventDispatcher() {}

void EventDispatcher::Add(const event_t &e) {
  scoped_mutex_lock lock(async_lock);
  events.push_back(e);
}

void EventDispatcher::Flush(Napi::Env env) {
  
  // Iterate through each of the currently stored events
  // generate a callback object for each, setting to the members
  // then
  if (events.size() < 1) return;

  const unsigned int argc = 2;

  std::vector<event_t> _events;
  {
    scoped_mutex_lock lock(async_lock);
    events.swap(_events);
  }

  for (size_t i=0; i < _events.size(); i++) {
    Napi::Value argv[argc] = {};
    Napi::Object jsobj = Napi::Object::New(env);

    switch (_events[i].type) {
      case RdKafka::Event::EVENT_ERROR:
        argv[0] = Napi::String::New(env, "error");
        argv[1] = Napi::String::New(env, _events[i].message.c_str());

        // if (event->err() == RdKafka::ERR__ALL_BROKERS_DOWN). Stop running
        // This may be better suited to the node side of things
        break;
      case RdKafka::Event::EVENT_STATS:
        argv[0] = Napi::String::New(env, "stats");

        jsobj.Set("message",
          Napi::String::New(env, _events[i].message.c_str()));

        break;
      case RdKafka::Event::EVENT_LOG:
        argv[0] = Napi::String::New(env, "log");

        jsobj.Set("severity",
          Napi::Number::New(env, _events[i].severity));
        jsobj.Set("fac",
          Napi::String::New(env, _events[i].fac.c_str()));
        jsobj.Set("message",
          Napi::String::New(env, _events[i].message.c_str()));

        break;
      case RdKafka::Event::EVENT_THROTTLE:
        argv[0] = Napi::String::New(env, "throttle");

        jsobj.Set("message",
          Napi::String::New(env, _events[i].message.c_str()));

        jsobj.Set("throttleTime",
          Napi::Number::New(env, _events[i].throttle_time));
        jsobj.Set("brokerName",
          Napi::String::New(env, _events[i].broker_name));
        jsobj.Set("brokerId",
          Napi::Number::New(env, _events[i].broker_id));

        break;
      default:
        argv[0] = Napi::String::New(env, "event");

        jsobj.Set("message",
          Napi::String::New(env, events[i].message.c_str()));

        break;
    }

    if (_events[i].type != RdKafka::Event::EVENT_ERROR) {
      // error would be assigned already
      argv[1] = jsobj;
    }

    Dispatch({argv[0],argv[1]});
  }
}

DeliveryReportDispatcher::DeliveryReportDispatcher() {}
DeliveryReportDispatcher::~DeliveryReportDispatcher() {}

size_t DeliveryReportDispatcher::Add(const DeliveryReport &e) {
  scoped_mutex_lock lock(async_lock);
  events.push_back(e);
  return events.size();
}

void DeliveryReportDispatcher::Flush(Napi::Env env) {
  

  const unsigned int argc = 2;

  size_t outstanding_event_count = 0;
  std::vector<DeliveryReport> events_list;
  {
    scoped_mutex_lock lock(async_lock);
    outstanding_event_count = events.size();
    const size_t flush_count = std::min<size_t>(outstanding_event_count, 100UL);
    events_list.reserve(flush_count);
    for (size_t i = 0; i < flush_count; i++) {
      events_list.emplace_back(std::move(events.front()));
      events.pop_front();
    }
  }

  for (size_t i = 0; i < events_list.size(); i++) {
    Napi::Value argv[argc] = {};

    const DeliveryReport& event = events_list[i];

    if (event.is_error) {
        // If it is an error we need the first argument to be set
        argv[0] = Napi::String::New(env, event.error_string.c_str());
    } else {
        argv[0] = env.Null();
    }
    Napi::Object jsobj(Napi::Object::New(env));

    jsobj.Set("topic",
            Napi::String::New(env, event.topic_name));
    jsobj.Set("partition",
            Napi::Number::New(env, event.partition));
    jsobj.Set("offset",
            Napi::Number::New(env, event.offset));

    if (event.key) {
      Napi::Buffer<char> buff = Napi::Buffer<char>::New(
        env, static_cast<char*>(event.key), static_cast<int>(event.key_len)
      );
      jsobj.Set("key",
              buff);
    } else {
      jsobj.Set("key", env.Null());
    }

    if (event.opaque) {//TODO support opaque
      // Nan::Persistent<v8::Value> * persistent =
      //   static_cast<Nan::Persistent<v8::Value> *>(event.opaque);
      // Napi::Value object = Napi::String::New(env, *persistent);
      // jsobj.Set("opaque", object);

      // // Okay... now reset and destroy the persistent handle
      // persistent->Reset();

      // // Get rid of the persistent since we are making it local
      // delete persistent;
    }

    if (event.timestamp > -1) {
      jsobj.Set("timestamp",
              Napi::Number::New(env, event.timestamp));
    }

    if (event.m_include_payload) {
        if (event.payload) {
          Napi::Buffer<char> buff = Napi::Buffer<char>::New(
          env, static_cast<char*>(event.key), static_cast<int>(event.key_len)
        );


        jsobj.Set("value",
          buff);
      } else {
        jsobj.Set("value",
          env.Null());
      }
    }

    jsobj.Set("size",
            Napi::Number::New(env, event.len));

    argv[1] = jsobj;

    Dispatch({argv[0], argv[1]});
  }
  if (outstanding_event_count > events_list.size()) {
    Execute();
  }
}

// This only exists to circumvent the problem with not being able to execute JS
// on any thread other than the main thread.

// I still think there may be better alternatives, because there is a lot of
// duplication here
DeliveryReport::DeliveryReport(RdKafka::Message &message, bool include_payload) :  // NOLINT
  m_include_payload(include_payload) {
  if (message.err() == RdKafka::ERR_NO_ERROR) {
    is_error = false;
  } else {
    is_error = true;
    error_code = message.err();
    error_string = message.errstr();
  }

  topic_name = message.topic_name();
  partition = message.partition();
  offset = message.offset();

  if (message.timestamp().type !=
    RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
    timestamp = message.timestamp().timestamp;
  } else {
    timestamp = -1;
  }


  // Key length.
  key_len = message.key_len();

  // It is okay if this is null
  if (message.key_pointer()) {
    key = malloc(message.key_len());
    memcpy(key, message.key_pointer(), message.key_len());
  } else {
    key = NULL;
  }

  if (message.msg_opaque()) {
    opaque = message.msg_opaque();
  } else {
    opaque = NULL;
  }

  len = message.len();

  if (m_include_payload && message.payload()) {
    // this pointer will be owned and freed by the Nan::NewBuffer
    // created in DeliveryReportDispatcher::Flush(Napi::Env env)
    payload = malloc(len);
    memcpy(payload, message.payload(), len);
  } else {
    payload = NULL;
  }
}

DeliveryReport::~DeliveryReport() {}

// Delivery Report

Delivery::Delivery():
  dispatcher() {
    m_dr_msg_cb = false;
  }
Delivery::~Delivery() {}


void Delivery::SendMessageBuffer(bool send_dr_msg) {
  m_dr_msg_cb = true;
}

void Delivery::dr_cb(RdKafka::Message &message) {
  if (!dispatcher.HasCallbacks()) {
    return;
  }

  DeliveryReport msg(message, m_dr_msg_cb);
  if (dispatcher.Add(msg) == 1) {
    dispatcher.Execute();
  }
}

// Rebalance CB

RebalanceDispatcher::RebalanceDispatcher() {}
RebalanceDispatcher::~RebalanceDispatcher() {}

void RebalanceDispatcher::Add(const rebalance_event_t &e) {
  scoped_mutex_lock lock(async_lock);
  m_events.push_back(e);
}

void RebalanceDispatcher::Flush(Napi::Env env) {
  
  // Iterate through each of the currently stored events
  // generate a callback object for each, setting to the members
  // then

  if (m_events.size() < 1) return;

  const unsigned int argc = 2;

  std::vector<rebalance_event_t> events;
  {
    scoped_mutex_lock lock(async_lock);
    m_events.swap(events);
  }

  for (size_t i=0; i < events.size(); i++) {
    Napi::Value argv[argc] = {};

    if (events[i].err == RdKafka::ERR_NO_ERROR) {
      argv[0] = env.Undefined();
    } else {
      // ERR__ASSIGN_PARTITIONS? Special case? Nah
      argv[0] = Napi::Number::New(env, events[i].err);
    }

    std::vector<event_topic_partition_t> parts = events[i].partitions;

    // Now convert the TopicPartition list to a JS array
    argv[1] = TopicPartitionListToV8Array(env, events[i].partitions);

    Dispatch({argv[0], argv[1]});
  }
}

void Rebalance::rebalance_cb(RdKafka::KafkaConsumer *consumer,
    RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*> &partitions) {
  dispatcher.Add(rebalance_event_t(err, partitions));
  dispatcher.Execute();
}

// Offset Commit CB

OffsetCommitDispatcher::OffsetCommitDispatcher() {}
OffsetCommitDispatcher::~OffsetCommitDispatcher() {}

void OffsetCommitDispatcher::Add(const offset_commit_event_t &e) {
  scoped_mutex_lock lock(async_lock);
  m_events.push_back(e);
}

void OffsetCommitDispatcher::Flush(Napi::Env env) {
  
  // Iterate through each of the currently stored events
  // generate a callback object for each, setting to the members
  // then

  if (m_events.size() < 1) return;

  const unsigned int argc = 2;

  std::vector<offset_commit_event_t> events;
  {
    scoped_mutex_lock lock(async_lock);
    m_events.swap(events);
  }

  for (size_t i = 0; i < events.size(); i++) {
    Napi::Value argv[argc] = {};

    if (events[i].err == RdKafka::ERR_NO_ERROR) {
      argv[0] = env.Undefined();
    } else {
      argv[0] = Napi::Number::New(env, events[i].err);
    }

    // Now convert the TopicPartition list to a JS array
    argv[1] = TopicPartitionListToV8Array(env, events[i].partitions);

    Dispatch({argv[0], argv[1]});
  }
}

void OffsetCommit::offset_commit_cb(RdKafka::ErrorCode err,
    std::vector<RdKafka::TopicPartition*> &offsets) {
  dispatcher.Add(offset_commit_event_t(err, offsets));
  dispatcher.Execute();
}

// Partitioner callback

Partitioner::Partitioner() {}
Partitioner::~Partitioner() {}

int32_t Partitioner::partitioner_cb(//TODO not support
                                    const RdKafka::Topic *topic,
                                    const std::string *key,
                                    int32_t partition_cnt,
                                    void *msg_opaque) {
  // Send this and get the callback and parse the int
  // if (callback.IsEmpty()) {
  //   // default behavior
  //   return random(topic, partition_cnt);
  // }

  // Napi::Value argv[3] = {};

  // argv[0] = Napi::String::New(env, topic->name().c_str());
  // if (key->empty()) {
  //   argv[1] = env.Null();
  // } else {
  //   argv[1] = Napi::String::New(env, key->c_str());
  // }

  // argv[2] = Napi::Number::New(env, partition_cnt);

  // Napi::Value return_value = callback.Call({argv[0], argv[1], argv[2]});

  // Napi::Number partition_return = return_value.ToNumber();

  // int32_t chosen_partition;

  // if (partition_return.IsEmpty()) {
  //   chosen_partition = RdKafka::Topic::PARTITION_UA;
  // } else {
  //    chosen_partition = partition_return.Int32Value();
  // }

  if (!topic->partition_available(partition_cnt)) {
    return RdKafka::Topic::PARTITION_UA;
  }

  return -1;//chosen_partition;
}

int32_t Partitioner::partitionerCallback(Napi::Env env,
                                    const RdKafka::Topic *topic,
                                    const std::string *key,
                                    int32_t partition_cnt,
                                    void *msg_opaque) {
  // Send this and get the callback and parse the int
  if (callback.IsEmpty()) {
    // default behavior
    return random(topic, partition_cnt);
  }

  Napi::Value argv[3] = {};

  argv[0] = Napi::String::New(env, topic->name().c_str());
  if (key->empty()) {
    argv[1] = env.Null();
  } else {
    argv[1] = Napi::String::New(env, key->c_str());
  }

  argv[2] = Napi::Number::New(env, partition_cnt);

  Napi::Value return_value = callback.Call({argv[0], argv[1], argv[2]});

  Napi::Number partition_return = return_value.ToNumber();

  int32_t chosen_partition;

  if (partition_return.IsEmpty()) {
    chosen_partition = RdKafka::Topic::PARTITION_UA;
  } else {
     chosen_partition = partition_return.Int32Value();
  }

  if (!topic->partition_available(chosen_partition)) {
    return RdKafka::Topic::PARTITION_UA;
  }

  return chosen_partition;
}

unsigned int Partitioner::djb_hash(const char *str, size_t len) {
  unsigned int hash = 5381;
  for (size_t i = 0 ; i < len ; i++)
    hash = ((hash << 5) + hash) + str[i];
  return hash;
}

unsigned int Partitioner::random(const RdKafka::Topic *topic, int32_t max) {
  int32_t random_partition = rand() % max;  // NOLINT

  if (topic->partition_available(random_partition)) {
    return random_partition;
  } else {
    return RdKafka::Topic::PARTITION_UA;
  }
}

void Partitioner::SetCallback(Napi::Function cb) {
  callback = cb;
}


}  // end namespace Callbacks

}  // End namespace NodeKafka
