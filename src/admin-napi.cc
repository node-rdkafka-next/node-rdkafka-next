/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <napi.h>
#include <string>
#include <vector>

#include "src/admin-napi.h"
#include "src/workers.h"

namespace NodeKafka {

/**
 * @brief AdminClient v8 wrapped object.
 *
 * Specializes the connection to wrap a consumer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

AdminClientNapi::AdminClientNapi(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<AdminClientNapi>(info) {
  Napi::Env env = info.Env();
  if (!info.IsConstructCall()) {
    Napi::TypeError::New(env, "non-constructor invocation not supported")
        .ThrowAsJavaScriptException();
    return;
  }

  if (info.Length() < 1) {
    Napi::TypeError::New(env, "You must supply a global configuration")
        .ThrowAsJavaScriptException();
    return;
  }

  if (!info[0].IsObject()) {
    Napi::TypeError::New(env, "Global configuration data must be specified")
        .ThrowAsJavaScriptException();
    return;
  }

  std::string errstr;

  Conf *gconfig =
      Conf::create(RdKafka::Conf::CONF_GLOBAL, (info[0].ToObject()), errstr);

  if (!gconfig) {
    Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
    return;
  }

  client = new AdminClient(gconfig);
}
void AdminClientNapi::Finalize(Napi::Env env) { delete client; }

Napi::Object AdminClientNapi::Init(Napi::Env env, Napi::Object exports) {

  Napi::Function func = DefineClass(
      env, "AdminClient",
      {
          InstanceMethod("createTopic", &AdminClientNapi::NodeCreateTopic),
          InstanceMethod("deleteTopic", &AdminClientNapi::NodeDeleteTopic),
          InstanceMethod("createPartitions", &AdminClientNapi::NodeDeleteTopic),
          InstanceMethod("connect", &AdminClientNapi::NodeConnect),
          InstanceMethod("disconnect", &AdminClientNapi::NodeDisconnect),
      });

  Napi::FunctionReference *constructor = new Napi::FunctionReference();
  *constructor = Napi::Persistent(func);
  env.SetInstanceData(constructor);

  exports.Set("AdminClient", func);
  return exports;
}

Napi::Object AdminClientNapi::NewInstance(Napi::Env env, Napi::Value arg) {
  Napi::EscapableHandleScope scope(env);

  Napi::Object obj = env.GetInstanceData<Napi::FunctionReference>()->New({arg});
  return scope.Escape(napi_value(obj)).ToObject();
}

/**
 * @section
 * C++ Exported prototype functions
 */

Napi::Value AdminClientNapi::NodeConnect(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  Baton b = this->client->Connect();
  // Let the JS library throw if we need to so the error can be more rich
  int error_code = static_cast<int>(b.err());
  return Napi::Number::New(env, error_code);
}

Napi::Value AdminClientNapi::NodeDisconnect(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  Baton b = this->client->Disconnect();
  // Let the JS library throw if we need to so the error can be more rich
  int error_code = static_cast<int>(b.err());
  return Napi::Number::New(env, error_code);
}

/**
 * Create topic
 */
Napi::Value AdminClientNapi::NodeCreateTopic(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 3 || !info[2].IsFunction()) {
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[1].IsNumber()) {
    Napi::TypeError::New(env, "Must provide 'timeout'")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // Create the final callback object
  Napi::Function cb = info[2].As<Napi::Function>();
  // Napi::Function& callback = new Nan::Callback(cb);
  // AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Get the timeout
  int timeout = info[1].As<Napi::Number>().Int32Value();

  std::string errstr;
  // Get that topic we want to create
  rd_kafka_NewTopic_t *topic =
      Conversion::Admin::FromV8TopicObject(info[0].As<Napi::Object>(), errstr);

  if (topic == NULL) {
    Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
    return env.Null();
  }
  // Queue up dat work

  new Workers::AdminClientCreateTopic(cb, client, topic, timeout);

  return env.Null();
}

/**
 * Delete topic
 */
Napi::Value AdminClientNapi::NodeDeleteTopic(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 3 || !info[2].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[1].IsNumber() || !info[0].IsString()) {
    Napi::TypeError::New(env, "Must provide 'timeout', and 'topicName'")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // Create the final callback object
  Napi::Function cb = info[2].As<Napi::Function>();

  // Get the topic name from the string
  std::string topic_name = Util::FromV8String(info[0].As<Napi::String>());

  // Get the timeout
  int timeout = (info[2]).As<Napi::Number>().Int32Value();

  // Get that topic we want to create
  rd_kafka_DeleteTopic_t *topic = rd_kafka_DeleteTopic_new(topic_name.c_str());

  // Queue up dat work

  new Workers::AdminClientDeleteTopic(cb, client, topic, timeout);

  return env.Null();
}

/**
 * Delete topic
 */
Napi::Value
AdminClientNapi::NodeCreatePartitions(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 4) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[3].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback 2")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[2].IsNumber() || !info[1].IsNumber() || !info[0].IsString()) {
    Napi::TypeError::New(
        env, "Must provide 'totalPartitions', 'timeout', and 'topicName'");
    return env.Null();
  }

  // Create the final callback object
  Napi::Function cb = info[3].As<Napi::Function>();
  // AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Get the total number of desired partitions
  int partition_total_count =
      info[1]
          .As<Napi::Number>()
          .Int32Value(); // info[1].As<Napi::Number>().Int32Value();

  // Get the topic name from the string
  std::string topic_name = info[0].As<Napi::String>();

  // Create an error buffer we can throw
  char *errbuf = reinterpret_cast<char *>(malloc(100));

  // Create the new partitions request
  rd_kafka_NewPartitions_t *new_partitions = rd_kafka_NewPartitions_new(
      topic_name.c_str(), partition_total_count, errbuf, 100);

  // If we got a failure on the create new partitions request,
  // fail here
  if (new_partitions == NULL) {
    Napi::TypeError::New(env, errbuf).ThrowAsJavaScriptException();
    return env.Null();
  }

  // Queue up dat work
  new Workers::AdminClientCreatePartitions(cb, this->client, new_partitions,
                                           1000);

  return env.Null();
}

} // namespace NodeKafka
