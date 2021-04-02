/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <iostream>
#include "src/binding.h"

using NodeKafka::ProducerNapi;
using NodeKafka::KafkaConsumerNapi;
using NodeKafka::AdminClientNapi;
using NodeKafka::TopicNapi;

using node::AtExit;
using RdKafka::ErrorCode;

static void RdKafkaCleanup(void*) {  // NOLINT
  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */

  RdKafka::wait_destroyed(5000);
}
Napi::String NodeRdKafkaErr2Str(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  int points = info[0].As<Napi::Number>().Int32Value();
  // Cast to error code
  RdKafka::ErrorCode err = static_cast<RdKafka::ErrorCode>(points);

  std::string errstr = RdKafka::err2str(err);

  return Napi::String::New(env, errstr);
}

Napi::Value  NodeRdKafkaBuildInFeatures(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  RdKafka::Conf * config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  std::string features;
  Napi::Value value; 
  if (RdKafka::Conf::CONF_OK == config->get("builtin.features", features)) {
    // Napi::String::New(env, features);
    value = Napi::String::New(env, features);
  } else {
    value = env.Undefined();
  }

  delete config;
  return value;
}

void ConstantsInit(Napi::Env env, Napi::Object exports) {
  Napi::Object topicConstants = Napi::Object::New(env);
  // Napi::Object topicConstants = Napi::Object::New(env);
  topicConstants.DefineProperties({
    Napi::PropertyDescriptor::Value("PARTITION_UA", Napi::Number::New(env, RdKafka::Topic::PARTITION_UA)),
    Napi::PropertyDescriptor::Value("OFFSET_BEGINNING", Napi::Number::New(env, RdKafka::Topic::OFFSET_BEGINNING)),
    Napi::PropertyDescriptor::Value("OFFSET_END", Napi::Number::New(env, RdKafka::Topic::OFFSET_END)),
    Napi::PropertyDescriptor::Value("OFFSET_STORED", Napi::Number::New(env, RdKafka::Topic::OFFSET_STORED)),
    Napi::PropertyDescriptor::Value("OFFSET_INVALID", Napi::Number::New(env, RdKafka::Topic::OFFSET_INVALID)),
  });
  // RdKafka Error Code definitions
  

  exports.Set(Napi::String::New(env,"topic"), topicConstants);

  exports.Set(Napi::String::New(env,"err2str"),
    Napi::Function::New(env, NodeRdKafkaErr2Str));  // NOLINT

  exports.Set(Napi::String::New(env,"features"),
    Napi::Function::New(env, NodeRdKafkaBuildInFeatures));  // NOLINT
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
#if NODE_MAJOR_VERSION <= 9 || (NODE_MAJOR_VERSION == 10 && NODE_MINOR_VERSION <= 15)
  AtExit(RdKafkaCleanup);
#else
  // v8::Local<v8::Context> context = Nan::GetCurrentContext();
  // node::Environment* env = node::GetCurrentEnvironment(context);
  // AtExit(env, RdKafkaCleanup, NULL);
  napi_add_env_cleanup_hook(env, RdKafkaCleanup, NULL);
#endif
  KafkaConsumerNapi::Init(env, exports);
  ProducerNapi::Init(env, exports);
  AdminClientNapi::Init(env, exports);
  TopicNapi::Init(env, exports);
  ConstantsInit(env, exports);

  exports.Set(Napi::String::New(env,"librdkafkaVersion"),
      Napi::String::New(env, RdKafka::version_str().c_str()));
  return exports;
}

NODE_API_MODULE(kafka, Init)
