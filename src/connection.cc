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

#include "src/connection.h"
#include "src/workers.h"

using RdKafka::Conf;

namespace NodeKafka {

/**
 * @brief Connection v8 wrapped object.
 *
 * Wraps the RdKafka::Handle object with compositional inheritence and
 * provides sensible defaults for exposing callbacks to node
 *
 * This object can't itself expose methods to the prototype directly, as far
 * as I can tell. But it can provide the NAN_METHODS that just need to be added
 * to the prototype. Since connections, etc. are managed differently based on
 * whether it is a producer or consumer, they manage that. This base class
 * handles some of the wrapping functionality and more importantly, the
 * configuration of callbacks
 *
 * Any callback available to both consumers and producers, like logging or
 * events will be handled in here.
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

Connection::Connection(Conf* gconfig, Conf* tconfig):
  m_event_cb(),
  m_gconfig(gconfig),
  m_tconfig(tconfig) {
    std::string errstr;

    m_client = NULL;
    m_is_closing = false;
    uv_rwlock_init(&m_connection_lock);

    // Try to set the event cb. Shouldn't be an error here, but if there
    // is, it doesn't get reported.
    //
    // Perhaps node new methods should report this as an error? But there
    // isn't anything the user can do about it.
    m_gconfig->set("event_cb", &m_event_cb, errstr);
  }

Connection::~Connection() {
  uv_rwlock_destroy(&m_connection_lock);

  if (m_tconfig) {
    delete m_tconfig;
  }

  if (m_gconfig) {
    delete m_gconfig;
  }
}

RdKafka::TopicPartition* Connection::GetPartition(std::string &topic) {
  return RdKafka::TopicPartition::create(topic, RdKafka::Topic::PARTITION_UA);
}

RdKafka::TopicPartition* Connection::GetPartition(std::string &topic, int partition) {  // NOLINT
  return RdKafka::TopicPartition::create(topic, partition);
}

bool Connection::IsConnected() {
  return !m_is_closing && m_client != NULL;
}

bool Connection::IsClosing() {
  return m_client != NULL && m_is_closing;
}

RdKafka::Handle* Connection::GetClient() {
  return m_client;
}

Baton Connection::CreateTopic(std::string topic_name) {
  return CreateTopic(topic_name, NULL);
}

Baton Connection::CreateTopic(std::string topic_name, RdKafka::Conf* conf) {
  std::string errstr;

  RdKafka::Topic* topic = NULL;

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      topic = RdKafka::Topic::create(m_client, topic_name, conf, errstr);
    } else {
      return Baton(RdKafka::ErrorCode::ERR__STATE);
    }
  } else {
    return Baton(RdKafka::ErrorCode::ERR__STATE);
  }

  if (!errstr.empty()) {
    return Baton(RdKafka::ErrorCode::ERR_TOPIC_EXCEPTION, errstr);
  }

  // Maybe do it this way later? Then we don't need to do static_cast
  // <RdKafka::Topic*>
  return Baton(topic);
}

Baton Connection::QueryWatermarkOffsets(
  std::string topic_name, int32_t partition,
  int64_t* low_offset, int64_t* high_offset,
  int timeout_ms) {
  // Check if we are connected first

  RdKafka::ErrorCode err;

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      // Always send true - we
      err = m_client->query_watermark_offsets(topic_name, partition,
        low_offset, high_offset, timeout_ms);

    } else {
      err = RdKafka::ERR__STATE;
    }
  } else {
    err = RdKafka::ERR__STATE;
  }

  return Baton(err);
}

/**
 * Look up the offsets for the given partitions by timestamp.
 *
 * The returned offset for each partition is the earliest offset whose
 * timestamp is greater than or equal to the given timestamp in the
 * corresponding partition.
 *
 * @returns A baton specifying the error state. If there was no error,
 *          there still may be an error on a topic partition basis.
 */
Baton Connection::OffsetsForTimes(
  std::vector<RdKafka::TopicPartition*> &toppars,
  int timeout_ms) {
  // Check if we are connected first

  RdKafka::ErrorCode err;

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      // Always send true - we
      err = m_client->offsetsForTimes(toppars, timeout_ms);

    } else {
      err = RdKafka::ERR__STATE;
    }
  } else {
    err = RdKafka::ERR__STATE;
  }

  return Baton(err);
}

Baton Connection::GetMetadata(
  bool all_topics, std::string topic_name, int timeout_ms) {
  RdKafka::Topic* topic = NULL;
  RdKafka::ErrorCode err;

  std::string errstr;

  if (!topic_name.empty()) {
    Baton b = CreateTopic(topic_name);
    if (b.err() == RdKafka::ErrorCode::ERR_NO_ERROR) {
      topic = b.data<RdKafka::Topic*>();
    }
  }

  RdKafka::Metadata* metadata = NULL;

  if (!errstr.empty()) {
    return Baton(RdKafka::ERR_TOPIC_EXCEPTION);
  }

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      // Always send true - we
      err = m_client->metadata(all_topics, topic, &metadata, timeout_ms);
    } else {
      err = RdKafka::ERR__STATE;
    }
  } else {
    err = RdKafka::ERR__STATE;
  }

  if (err == RdKafka::ERR_NO_ERROR) {
    return Baton(metadata);
  } else {
    // metadata is not set here
    // @see https://github.com/edenhill/librdkafka/blob/master/src-cpp/rdkafkacpp.h#L860
    return Baton(err);
  }
}

void Connection::ConfigureCallback(Napi::Env env,const std::string &string_key, const Napi::Function &cb, bool add) {
  if (string_key.compare("event_cb") == 0) {
    if (add) {
      this->m_event_cb.dispatcher.AddCallback(cb);
    } else {
      this->m_event_cb.dispatcher.RemoveCallback(cb);
    }
  }
}

// NAN METHODS

Napi::Value Connection::NodeGetMetadata(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();


  Napi::Object config;
  if (info[0].IsObject()) {
    config = info[0].As<Napi::Object>();
  } else {
    config = Napi::Object::New(env);
  }

  if (!info[1].IsFunction()) {
    Napi::TypeError::New(env, "Second parameter must be a callback").ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Function cb = info[1].As<Napi::Function>();

  std::string topic = GetParameter<std::string>(config, "topic", "");
  bool allTopics = GetParameter<bool>(config, "allTopics", true);
  int timeout_ms = GetParameter<int64_t>(config, "timeout", 30000);


  new Workers::ConnectionMetadata(
    cb, this, topic, timeout_ms, allTopics
  );
  return env.Null();
}

Napi::Value Connection::NodeOffsetsForTimes(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 3 || !info[0].IsArray()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify an array of topic partitions").ThrowAsJavaScriptException();
    return env.Null();
  }

  std::vector<RdKafka::TopicPartition *> toppars =
    Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

  Napi::Number timeoutMs = (info[1].As<Napi::Number>());
  uint32_t timeout_ms;
  uint32_t DEFAULT_TIMEOUT = 1000;
  if (timeoutMs.IsEmpty()) {
    timeout_ms = DEFAULT_TIMEOUT;
  } else {
    timeout_ms = timeoutMs.Int32Value();
    if (timeout_ms <=0) {
      timeout_ms = DEFAULT_TIMEOUT;
    }
  }

  Napi::Function cb = info[2].As<Napi::Function>();
  new Workers::Handle::OffsetsForTimes(cb, this,
      toppars, timeout_ms);
  return env.Null();
}

Napi::Value Connection::NodeQueryWatermarkOffsets(const Napi::CallbackInfo& info) {
  
  Napi::Env env = info.Env();
  // Connection* obj = ObjectWrap::Unwrap<Connection>(info.This());

  if (!info[0].IsString()) {
    Napi::TypeError::New(env, "1st parameter must be a topic string").ThrowAsJavaScriptException();;
    return env.Null();;
  }

  if (!info[1].IsNumber()) {
    Napi::TypeError::New(env, "2nd parameter must be a partition number").ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[2].IsNumber()) {
    Napi::TypeError::New(env, "3rd parameter must be a number of milliseconds").ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[3].IsFunction()) {
    Napi::TypeError::New(env, "4th parameter must be a callback").ThrowAsJavaScriptException();
    return env.Null();
  }

  // Get string pointer for the topic name
  // The first parameter is the topic
  std::string topic_name = info[0].As<Napi::String>().Utf8Value();

  // Second parameter is the partition
  int32_t partition = info[1].As<Napi::Number>().Int32Value();

  // Third parameter is the timeout
  int timeout_ms = info[2].As<Napi::Number>().Int32Value();

  // Fourth parameter is the callback
  Napi::Function cb = info[3].As<Napi::Function>();

  new Workers::ConnectionQueryWatermarkOffsets(
    cb, this, topic_name, partition, timeout_ms);
  return env.Null();
}

// Node methods
Napi::Value Connection::NodeConfigureCallbacks(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (info.Length() < 2 ||
    !info[0].IsBoolean() ||
    !info[1].IsObject()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callbacks object").ThrowAsJavaScriptException();
    return env.Null();
  }

  const bool add = info[0].As<Napi::Boolean>().Value();
  Napi::Object configs_object = info[1].ToObject();
  Napi::Array configs_property_names = configs_object.GetPropertyNames();

  for (unsigned int j = 0; j < configs_property_names.Length(); ++j) {
    std::string configs_string_key;

    Napi::Value configs_key = configs_property_names.Get(j);
    Napi::Value configs_value = configs_object.Get(configs_key);

    int config_type = 0;
    if (configs_value.IsObject() && configs_key.IsString()) {
      configs_string_key = configs_key.ToString().Utf8Value();
      if (configs_string_key.compare("global") == 0) {
          config_type = 1;
      } else if (configs_string_key.compare("topic") == 0) {
          config_type = 2;
      } else if (configs_string_key.compare("event") == 0) {
          config_type = 3;
      } else {
        continue;
      }
    } else {
      continue;
    }

    Napi::Object object = configs_value.ToObject();
    Napi::Array property_names = object.GetPropertyNames();

    for (unsigned int i = 0; i < property_names.Length(); ++i) {
      std::string errstr;
      std::string string_key;

      Napi::Value key = property_names.Get(i);
      Napi::Value value = object.Get(key);

      if (key.IsString()) {
        string_key = key.ToString().Utf8Value();
      } else {
        continue;
      }

      if (value.IsFunction()) {
        Napi::Function cb = value.As<Napi::Function>();
        switch (config_type) {
          case 1:
            m_gconfig->ConfigureCallback(string_key, cb, add, errstr);
            if (!errstr.empty()) {
              Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
              return env.Null();
            }
            break;
          case 2:
            m_tconfig->ConfigureCallback(string_key, cb, add, errstr);
            if (!errstr.empty()) {
              Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
              return env.Null();
            }
            break;
          case 3:
            ConfigureCallback(env, string_key, cb, add);
            break;
        }
      }
    }
  }
  return env.Null();
}

}  // namespace NodeKafka
