/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_ADMIN_H_
#define SRC_ADMIN_H_

#include <uv.h>
#include <iostream>
#include <string>
#include <vector>

#include "rdkafkacpp.h"
#include "rdkafka.h"  // NOLINT

#include "src/common.h"
#include "src/connection.h"
#include "src/callbacks.h"

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

class AdminClient : public Connection {
 public:
  void ActivateDispatchers();
  void DeactivateDispatchers();

  Baton Connect();
  Baton Disconnect();

  Baton CreateTopic(rd_kafka_NewTopic_t* topic, int timeout_ms);
  Baton DeleteTopic(rd_kafka_DeleteTopic_t* topic, int timeout_ms);
  Baton CreatePartitions(rd_kafka_NewPartitions_t* topic, int timeout_ms);
  // Baton AlterConfig(rd_kafka_NewTopic_t* topic, int timeout_ms);
  // Baton DescribeConfig(rd_kafka_NewTopic_t* topic, int timeout_ms);
  explicit AdminClient(Conf* globalConfig);
  ~AdminClient();
 protected:
 

  rd_kafka_queue_t* rkqu;

 private:
  // Node methods
  // Napi::Value NodeValidateTopic(const Napi::CallbackInfo& info);

};

}  // namespace NodeKafka

#endif  // SRC_ADMIN_H_
