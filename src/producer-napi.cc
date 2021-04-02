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

#include "src/producer-napi.h"
#include "src/workers.h"

namespace NodeKafka {

/**
 * @brief Producer v8 wrapped object.
 *
 * Wraps the RdKafka::Producer object with compositional inheritence and
 * provides methods for interacting with it exposed to node.
 *
 * The base wrappable RdKafka::Handle deals with most of the wrapping but
 * we still need to declare its prototype.
 *
 * @sa RdKafka::Producer
 * @sa NodeKafka::Connection
 */

ProducerNapi::ProducerNapi(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<ProducerNapi>(info) {
  Napi::Env env = info.Env();
  if (!info.IsConstructCall()) {printf("non-constructor invocation not supported");
    Napi::TypeError::New(env, "non-constructor invocation not supported")
        .ThrowAsJavaScriptException();
    
    return;
  }

  if (info.Length() < 2) {printf("You must supply global and topic configuration");
    Napi::TypeError::New(env, "You must supply global and topic configuration")
        .ThrowAsJavaScriptException();
    
    return;
  }

  if (!info[0].IsObject()) {printf("Global configuration data must be specified");
    Napi::TypeError::New(env, "Global configuration data must be specified")
        .ThrowAsJavaScriptException();
    
    return;
  }

  if (!info[1].IsObject()) {printf("Topic configuration must be specified");
    Napi::TypeError::New(env, "Topic configuration must be specified")
        .ThrowAsJavaScriptException();
    
    return;
  }

  std::string errstr;

  Conf *gconfig =
      Conf::create(RdKafka::Conf::CONF_GLOBAL, (info[0].ToObject()), errstr);

  if (!gconfig) {printf("%s", errstr.c_str());
    Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
    
    return;
  }

  Conf *tconfig =
      Conf::create(RdKafka::Conf::CONF_TOPIC, (info[1].ToObject()), errstr);

  if (!tconfig) {
    // No longer need this since we aren't instantiating anything
    delete gconfig;printf("%s", errstr.c_str());
    Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
    
    return;
  }

  producer = new Producer(gconfig, tconfig);
}

void ProducerNapi::Finalize(Napi::Env env) { delete producer; }

Napi::Object ProducerNapi::Init(Napi::Env env, Napi::Object exports) {

  Napi::Function func = DefineClass(
      env, "Producer",
      {

          /*
           * Lifecycle events inherited from NodeKafka::Connection
           *
           * @sa NodeKafka::Connection
           */

          InstanceMethod("configureCallbacks",
                         &ProducerNapi::NodeConfigureCallbacks),

          /*
           * @brief Methods to do with establishing state
           */

          InstanceMethod("connect", &ProducerNapi::NodeConnect),
          InstanceMethod("disconnect", &ProducerNapi::NodeDisconnect),
          InstanceMethod("getMetadata", &ProducerNapi::NodeGetMetadata),
          InstanceMethod("queryWatermarkOffsets",
                         &ProducerNapi::NodeQueryWatermarkOffsets), // NOLINT
          InstanceMethod("poll", &ProducerNapi::NodePoll),

          /*
           * @brief Methods exposed to do with message production
           */

          InstanceMethod("setPartitioner", &ProducerNapi::NodeSetPartitioner),
          InstanceMethod("produce", &ProducerNapi::NodeProduce),

          InstanceMethod("flush", &ProducerNapi::NodeFlush),
      });
  Napi::FunctionReference *constructor = new Napi::FunctionReference();
  *constructor = Napi::Persistent(func);
  env.SetInstanceData(constructor);

  exports.Set("Producer", func);
  return exports;
}

Napi::Value
ProducerNapi::NodeConfigureCallbacks(const Napi::CallbackInfo &info) {
  return producer->NodeConfigureCallbacks(info);
}
Napi::Value ProducerNapi::NodeGetMetadata(const Napi::CallbackInfo &info) {
  return producer->NodeGetMetadata(info);
}
Napi::Value
ProducerNapi::NodeQueryWatermarkOffsets(const Napi::CallbackInfo &info) {
  return producer->NodeQueryWatermarkOffsets(info);
}

Napi::Object ProducerNapi::NewInstance(Napi::Env env, Napi::Value arg) {
  Napi::EscapableHandleScope scope(env);

  Napi::Object obj = env.GetInstanceData<Napi::FunctionReference>()->New({arg});
  return scope.Escape(napi_value(obj)).ToObject();
}

/* Node exposed methods */

/**
 * @brief ProducerNapi::NodeProduce - produce a message through a producer
 *
 * This is a synchronous method. You may ask, "why?". The answer is because
 * there is no true value doing this asynchronously. All it does is degrade
 * performance. This method does not block - all it does is add a message
 * to a queue. In the case where the queue is full, it will return an error
 * immediately. The only way this method blocks is when you provide it a
 * flag to do so, which we never do.
 *
 * Doing it asynchronously eats up the libuv threadpool for no reason and
 * increases execution time by a very small amount. It will take two ticks of
 * the event loop to execute at minimum - 1 for executing it and another for
 * calling back the callback.
 *
 * @sa RdKafka::ProducerNapi::produce
 */
Napi::Value ProducerNapi::NodeProduce(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // Need to extract the message data here.
  if (info.Length() < 3) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a topic, partition, and message")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // Second parameter is the partition
  int32_t partition;

  if (info[1].IsNull() || info[1].IsUndefined()) {
    partition = RdKafka::Topic::PARTITION_UA;
  } else {
    partition = info[1].As<Napi::Number>().Int32Value();
  }

  if (partition < 0) {
    partition = RdKafka::Topic::PARTITION_UA;
  }

  size_t message_buffer_length;
  void *message_buffer_data;

  if (info[2].IsNull()) {
    // This is okay for whatever reason
    message_buffer_length = 0;
    message_buffer_data = NULL;
  } else if (!info[2].IsBuffer()) {
    Napi::TypeError::New(env, "Message must be a buffer or null")
        .ThrowAsJavaScriptException();
    return env.Null();
  } else {
    //  =
    Napi::Buffer<uint8_t> message_buffer_object =
        info[2].As<Napi::Buffer<uint8_t>>();

    // v8 handles the garbage collection here so we need to make a copy of
    // the buffer or assign the buffer to a persistent handle.

    // I'm not sure which would be the more performant option. I assume
    // the persistent handle would be but for now we'll try this one
    // which should be more memory-efficient and allow v8 to dispose of the
    // buffer sooner

    message_buffer_length = message_buffer_object.Length();
    message_buffer_data = message_buffer_object.Data();
    if (message_buffer_data == NULL) {
      // empty string message buffer should not end up as null message
      Napi::Buffer<uint8_t> message_buffer_object_emptystring =
          Napi::Buffer<uint8_t>::New(env, new uint8_t[0], 0);
      message_buffer_length = message_buffer_object_emptystring.Length();
      message_buffer_data = message_buffer_object_emptystring.Data();
    }
  }

  size_t key_buffer_length;
  const void *key_buffer_data;
  std::string key;

  if (info[3].IsNull() || info[3].IsUndefined()) {
    // This is okay for whatever reason
    key_buffer_length = 0;
    key_buffer_data = NULL;
  } else if (info[3].IsBuffer()) {
    Napi::Buffer<uint8_t> key_buffer_object =
        (info[3].As<Napi::Buffer<uint8_t>>());

    // v8 handles the garbage collection here so we need to make a copy of
    // the buffer or assign the buffer to a persistent handle.

    // I'm not sure which would be the more performant option. I assume
    // the persistent handle would be but for now we'll try this one
    // which should be more memory-efficient and allow v8 to dispose of the
    // buffer sooner

    key_buffer_length = key_buffer_object.Length();
    key_buffer_data = key_buffer_object.Data();
    if (key_buffer_data == NULL) {
      // empty string key buffer should not end up as null key
      Napi::Buffer<uint8_t> key_buffer_object_emptystring =
          Napi::Buffer<uint8_t>::New(env, new uint8_t[0], 0);
      key_buffer_length = key_buffer_object_emptystring.Length();
      key_buffer_data = key_buffer_object_emptystring.Data();
    }
  } else {
    // If it was a string just use the utf8 value.
    Napi::String val = info[3].As<Napi::String>();
    // Get string pointer for this thing
    key = (val.Utf8Value());

    key_buffer_data = key.data();
    key_buffer_length = key.length();
  }

  int64_t timestamp;

  if (info.Length() > 4 && !info[4].IsUndefined() && !info[4].IsNull()) {
    if (!info[4].IsNumber()) {
      Napi::TypeError::New(env, "Timestamp must be a number")
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    timestamp = info[4].ToNumber().Int64Value();
  } else {
    timestamp = 0;
  }

  void *opaque = NULL;
  // Opaque handling
  if (info.Length() > 5 && !info[5].IsUndefined()) {
    // TODO support Persistent Opaque
    // We need to create a persistent handle
    // opaque = new Nan::Persistent<v8::Value>(info[5]);
    // To get the local from this later,
    // Napi::Object object = Napi::String::New(env, persistent);
  }

  std::vector<RdKafka::Headers::Header> headers;
  if (info.Length() > 6 && !info[6].IsUndefined()) {
    Napi::Array v8Headers = info[6].As<Napi::Array>();

    if (v8Headers.Length() >= 1) {
      for (unsigned int i = 0; i < v8Headers.Length(); i++) {
        Napi::Object header = v8Headers.Get(i).ToObject();
        if (header.IsEmpty()) {
          continue;
        }

        Napi::Array props = header.GetPropertyNames();
        Napi::String v8Key = props.Get(uint32_t(0)).ToString();
        Napi::String v8Value = header.Get(v8Key).ToString();

        std::string key = v8Key.Utf8Value();
        std::string value = v8Value.Utf8Value();
        headers.push_back(
            RdKafka::Headers::Header(key, value.c_str(), value.size()));
      }
    }
  }

  // Let the JS library throw if we need to so the error can be more rich
  int error_code;

  if (info[0].IsString()) {
    // Get string pointer for this thing
    std::string topic_name = info[0].As<Napi::String>();
    RdKafka::Headers *rd_headers = RdKafka::Headers::create(headers);

    Baton b = producer->Produce(
        message_buffer_data, message_buffer_length, topic_name, partition,
        key_buffer_data, key_buffer_length, timestamp, opaque, rd_headers);

    error_code = static_cast<int>(b.err());
    if (error_code != 0 && rd_headers) {
      delete rd_headers;
    }
  } else {
    // First parameter is a topic OBJECT
    TopicNapi *topic = TopicNapi::Unwrap(info[0].As<Napi::Object>());

    // Unwrap it and turn it into an RdKafka::Topic*
    Baton topic_baton = topic->topic->toRDKafkaTopic(producer);

    if (topic_baton.err() != RdKafka::ERR_NO_ERROR) {
      // Let the JS library throw if we need to so the error can be more rich
      error_code = static_cast<int>(topic_baton.err());

      return Napi::Number::New(env, error_code);
    }

    RdKafka::Topic *rd_topic = topic_baton.data<RdKafka::Topic *>();

    Baton b = producer->Produce(message_buffer_data, message_buffer_length,
                                rd_topic, partition, key_buffer_data,
                                key_buffer_length, opaque);

    // Delete the topic when we are done.
    delete rd_topic;

    error_code = static_cast<int>(b.err());
  }

  return Napi::Number::New(env, error_code);
}

Napi::Value ProducerNapi::NodeSetPartitioner(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Function cb = info[0].As<Napi::Function>();
  producer->m_partitioner_cb.SetCallback(cb);
  return Napi::Boolean::New(env, true);
}

Napi::Value ProducerNapi::NodeConnect(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // This needs to be offloaded to libuv
  Napi::Function cb = info[0].As<Napi::Function>();

  new Workers::ProducerConnect(cb, producer);
  return env.Null();
}

Napi::Value ProducerNapi::NodePoll(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (!producer->IsConnected()) {
    Napi::TypeError::New(env, "Producer is disconnected")
        .ThrowAsJavaScriptException();
    return env.Null();
  } else {
    producer->Poll();
    return Napi::Boolean::New(env, true);
  }
}

Napi::Value ProducerNapi::NodeFlush(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 2 || !info[1].IsFunction() || !info[0].IsNumber()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a timeout and a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  int timeout_ms = info[0].As<Napi::Number>().Int32Value();

  Napi::Function cb = info[1].As<Napi::Function>();

  new Workers::ProducerFlush(cb, producer, timeout_ms);
  return env.Null();
}

Napi::Value ProducerNapi::NodeDisconnect(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Function cb = info[0].As<Napi::Function>();

  new Workers::ProducerDisconnect(cb, producer);
  return env.Null();
}

} // namespace NodeKafka
