/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_ADMIN_NAPI_H_
#define SRC_ADMIN_NAPI_H_
#include <napi.h>
#include <uv.h>
#include <iostream>
#include <string>
#include <vector>


#include "src/common.h"
#include "src/connection.h"
#include "src/callbacks.h"
#include "src/admin.h"

namespace NodeKafka {

/**
 * @brief KafkaConsumer v8 wrapped object.
 *
 * Specializes the connection to wrap a consumer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

class AdminClientNapi : public Napi::ObjectWrap<AdminClientNapi> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  static Napi::Object NewInstance(Napi::Env env, Napi::Value arg);
  AdminClientNapi(const Napi::CallbackInfo& info);
  // ~AdminClientNapi();
  void Finalize(Napi::Env env);

 protected:




  

 private:
  NodeKafka::AdminClient* client;
  // Node methods
  // Napi::Value NodeValidateTopic(const Napi::CallbackInfo& info);
  Napi::Value NodeCreateTopic(const Napi::CallbackInfo& info);
  Napi::Value NodeDeleteTopic(const Napi::CallbackInfo& info);
  Napi::Value NodeCreatePartitions(const Napi::CallbackInfo& info);

  Napi::Value NodeConnect(const Napi::CallbackInfo& info);
  Napi::Value NodeDisconnect(const Napi::CallbackInfo& info);
};

}  // namespace NodeKafka

#endif  // SRC_ADMIN_NAPI_H_
