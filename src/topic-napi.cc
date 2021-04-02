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

#include "src/common.h"
#include "src/connection.h"
#include "src/topic-napi.h"

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

TopicNapi::TopicNapi(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<TopicNapi>(info) {
  Napi::Env env = info.Env();
  if (!info.IsConstructCall()) {
    Napi::TypeError::New(env, "non-constructor invocation not supported")
        .ThrowAsJavaScriptException();
    return;
  }

  if (info.Length() < 1) {
    Napi::TypeError::New(env, "topic name is required")
        .ThrowAsJavaScriptException();
    return;
  }

  if (!info[0].IsString()) {
    Napi::TypeError::New(env, "Topic name must be a string")
        .ThrowAsJavaScriptException();
    return;
  }

  RdKafka::Conf *config = NULL;

  if (info.Length() >= 2 && !info[1].IsUndefined() && !info[1].IsNull()) {
    // If they gave us two parameters, or the 3rd parameter is null or
    // undefined, we want to pass null in for the config

    std::string errstr;
    if (!info[1].IsObject()) {
      Napi::TypeError::New(env, "Configuration data must be specified")
          .ThrowAsJavaScriptException();
      return;
    }

    config = Conf::create(RdKafka::Conf::CONF_TOPIC, (info[1].ToObject()),
                          errstr); // NOLINT

    if (!config) {
      Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
      return;
    }
  }

  std::string topic_name = info[0].As<Napi::String>().Utf8Value();

  topic = new Topic(topic_name, config);
}

void TopicNapi::Finalize(Napi::Env env) { delete topic; }

Napi::Object TopicNapi::Init(Napi::Env env, Napi::Object exports) {

  Napi::Function func =
      DefineClass(env, "Topic",
                  {
                      InstanceMethod("name", &TopicNapi::NodeGetName),
                  });

  Napi::FunctionReference *constructor = new Napi::FunctionReference();
  *constructor = Napi::Persistent(func);
  env.SetInstanceData(constructor);

  exports.Set("Topic", func);
  return exports;
}

// handle

Napi::Object TopicNapi::NewInstance(Napi::Env env, Napi::Value arg) {
  Napi::EscapableHandleScope scope(env);

  Napi::Object obj = env.GetInstanceData<Napi::FunctionReference>()->New({arg});
  return scope.Escape(napi_value(obj)).ToObject();
}

Napi::Value TopicNapi::NodeGetName(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  return Napi::String::New(env, topic->name());
}

Napi::Value TopicNapi::NodePartitionAvailable(const Napi::CallbackInfo &info) {
  // @TODO(sparente)
}

Napi::Value TopicNapi::NodeOffsetStore(const Napi::CallbackInfo &info) {
  // @TODO(sparente)
}

} // namespace NodeKafka
