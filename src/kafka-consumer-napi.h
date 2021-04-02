/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_KAFKA_CONSUMER_NAPI_H_
#define SRC_KAFKA_CONSUMER_NAPI_H_

#include <napi.h>
#include <uv.h>
#include <iostream>
#include <string>
#include <vector>

#include "rdkafkacpp.h"

#include "src/common.h"
#include "src/connection.h"
#include "src/callbacks.h"
#include "src/kafka-consumer.h"

namespace NodeKafka {

/**
 * @brief KafkaConsumerNapi v8 wrapped object.
 *
 * Specializes the connection to wrap a consumer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

class KafkaConsumerNapi : public Napi::ObjectWrap<KafkaConsumerNapi> {
 public:
  static void Init(Napi::Env env, Napi::Object exports);
  static Napi::Object NewInstance(Napi::Env env, Napi::Value arg);
  KafkaConsumerNapi(const Napi::CallbackInfo& info);
  void Finalize(Napi::Env env);

 protected:


 private:
  KafkaConsumer* consumer;

  // Node methods
  Napi::Value NodeConnect(const Napi::CallbackInfo& info);
  Napi::Value NodeSubscribe(const Napi::CallbackInfo& info);
  Napi::Value NodeDisconnect(const Napi::CallbackInfo& info);
  Napi::Value NodeAssign(const Napi::CallbackInfo& info);
  Napi::Value NodeUnassign(const Napi::CallbackInfo& info);
  Napi::Value NodeAssignments(const Napi::CallbackInfo& info);
  Napi::Value NodeUnsubscribe(const Napi::CallbackInfo& info);
  Napi::Value NodeCommit(const Napi::CallbackInfo& info);
  Napi::Value NodeCommitSync(const Napi::CallbackInfo& info);
  Napi::Value NodeOffsetsStore(const Napi::CallbackInfo& info);
  Napi::Value NodeCommitted(const Napi::CallbackInfo& info);
  Napi::Value NodePosition(const Napi::CallbackInfo& info);
  Napi::Value NodeSubscription(const Napi::CallbackInfo& info);
  Napi::Value NodeSeek(const Napi::CallbackInfo& info);
  Napi::Value NodeGetWatermarkOffsets(const Napi::CallbackInfo& info);
  Napi::Value NodeConsumeLoop(const Napi::CallbackInfo& info);
  Napi::Value NodeConsume(const Napi::CallbackInfo& info);

  Napi::Value NodePause(const Napi::CallbackInfo& info);
  Napi::Value NodeResume(const Napi::CallbackInfo& info);
  Napi::Value NodeOffsetsForTimes(const Napi::CallbackInfo& info);
  Napi::Value NodeConfigureCallbacks(const Napi::CallbackInfo& info);
  Napi::Value NodeGetMetadata(const Napi::CallbackInfo& info);
  Napi::Value NodeQueryWatermarkOffsets(const Napi::CallbackInfo& info);
};

}  // namespace NodeKafka

#endif  // SRC_KAFKA_CONSUMER_NAPI_H_
