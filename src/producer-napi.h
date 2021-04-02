/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_PRODUCER_NAPI_H_
#define SRC_PRODUCER_NAPI_H_

#include <napi.h>
#include <node.h>
#include <node_buffer.h>
#include <string>

#include "rdkafkacpp.h"

#include "src/common.h"
#include "src/connection.h"
#include "src/callbacks.h"
#include "src/topic-napi.h"
#include "src/producer.h"

namespace NodeKafka {



class ProducerNapi : public Napi::ObjectWrap<ProducerNapi> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object);
  static Napi::Object NewInstance(Napi::Env env, Napi::Value);
  ProducerNapi(const Napi::CallbackInfo& info);
  void Finalize(Napi::Env env);
 protected:



 private:
  Producer* producer;
  Napi::Value NodeProduce(const Napi::CallbackInfo& info);
  Napi::Value NodeSetPartitioner(const Napi::CallbackInfo& info);
  Napi::Value NodeConnect(const Napi::CallbackInfo& info);
  Napi::Value NodeDisconnect(const Napi::CallbackInfo& info);
  Napi::Value NodePoll(const Napi::CallbackInfo& info);
  #if RD_KAFKA_VERSION > 0x00090200
  Napi::Value NodeFlush(const Napi::CallbackInfo& info);
  #endif

  Napi::Value NodeConfigureCallbacks(const Napi::CallbackInfo& info);
  Napi::Value NodeGetMetadata(const Napi::CallbackInfo& info);
  Napi::Value NodeQueryWatermarkOffsets(const Napi::CallbackInfo& info);

};

}  // namespace NodeKafka

#endif  // SRC_PRODUCER_NAPI_H_
