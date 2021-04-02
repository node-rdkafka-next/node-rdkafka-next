/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_TOPIC_NAPI_H_
#define SRC_TOPIC_NAPI_H_

#include <napi.h>
#include <string>

#include "rdkafkacpp.h"

#include "src/config.h"
#include "src/topic.h"

namespace NodeKafka {

class TopicNapi : public Napi::ObjectWrap<TopicNapi> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  static Napi::Object NewInstance(Napi::Env env, Napi::Value arg);
  Topic* topic;
  TopicNapi(const Napi::CallbackInfo& info);
  void Finalize(Napi::Env env);
 protected:
  Napi::Value NodeGetMetadata(const Napi::CallbackInfo& info);

 private:
  



  Napi::Value NodeGetName(const Napi::CallbackInfo& info);
  Napi::Value NodePartitionAvailable(const Napi::CallbackInfo& info);
  Napi::Value NodeOffsetStore(const Napi::CallbackInfo& info);
};

}  // namespace NodeKafka

#endif  // SRC_TOPIC_NAPI_H_
