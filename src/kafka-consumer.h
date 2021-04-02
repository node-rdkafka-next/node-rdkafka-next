/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_KAFKA_CONSUMER_H_
#define SRC_KAFKA_CONSUMER_H_

#include <napi.h>
#include <uv.h>
#include <iostream>
#include <string>
#include <vector>

#include "rdkafkacpp.h"

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

class KafkaConsumer : public Connection {
 public:

  Baton Connect();
  Baton Disconnect();

  Baton Subscription();
  Baton Unsubscribe();
  bool IsSubscribed();

  Baton Pause(std::vector<RdKafka::TopicPartition*> &);
  Baton Resume(std::vector<RdKafka::TopicPartition*> &);

  // Asynchronous commit events
  Baton Commit(std::vector<RdKafka::TopicPartition*>);
  Baton Commit(RdKafka::TopicPartition*);
  Baton Commit();

  Baton OffsetsStore(std::vector<RdKafka::TopicPartition*> &);
  Baton GetWatermarkOffsets(std::string, int32_t, int64_t*, int64_t*);

  // Synchronous commit events
  Baton CommitSync(std::vector<RdKafka::TopicPartition*>);
  Baton CommitSync(RdKafka::TopicPartition*);
  Baton CommitSync();

  Baton Committed(std::vector<RdKafka::TopicPartition*> &, int timeout_ms);
  Baton Position(std::vector<RdKafka::TopicPartition*> &);

  Baton RefreshAssignments();

  bool HasAssignedPartitions();
  int AssignedPartitionCount();

  Baton Assign(std::vector<RdKafka::TopicPartition*>);
  Baton Unassign();

  Baton Seek(const RdKafka::TopicPartition &partition, int timeout_ms);

  std::string Name();

  Baton Subscribe(std::vector<std::string>);
  Baton Consume(int timeout_ms);

  void ActivateDispatchers();
  void DeactivateDispatchers();
  KafkaConsumer(Conf *, Conf *);
  ~KafkaConsumer();
  std::vector<RdKafka::TopicPartition*> m_partitions;
 protected:

  

 private:
  static void part_list_print(const std::vector<RdKafka::TopicPartition*>&);

  
  int m_partition_cnt;
  bool m_is_subscribed = false;


};

}  // namespace NodeKafka

#endif  // SRC_KAFKA_CONSUMER_H_
