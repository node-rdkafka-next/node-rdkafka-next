/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_PRODUCER_H_
#define SRC_PRODUCER_H_

#include <napi.h>
#include <node.h>
#include <node_buffer.h>
#include <string>

#include "rdkafkacpp.h"

#include "src/common.h"
#include "src/connection.h"
#include "src/callbacks.h"
#include "src/topic.h"

namespace NodeKafka {

class ProducerMessage {
 public:
  explicit ProducerMessage(Napi::Object, NodeKafka::Topic*);
  ~ProducerMessage();

  void* Payload();
  size_t Size();
  bool IsEmpty();
  RdKafka::Topic * GetTopic();

  std::string m_errstr;

  Topic * m_topic;
  int32_t m_partition;
  std::string m_key;

  void* m_buffer_data;
  size_t m_buffer_length;

  bool m_is_empty;
};

class Producer : public Connection {
 public:

  Baton Connect();
  void Disconnect();
  void Poll();
  #if RD_KAFKA_VERSION > 0x00090200
  Baton Flush(int timeout_ms);
  #endif

  Baton Produce(void* message, size_t message_size,
    RdKafka::Topic* topic, int32_t partition,
    const void* key, size_t key_len,
    void* opaque);

  Baton Produce(void* message, size_t message_size,
    std::string topic, int32_t partition,
    std::string* key,
    int64_t timestamp, void* opaque,
    RdKafka::Headers* headers);

  Baton Produce(void* message, size_t message_size,
    std::string topic, int32_t partition,
    const void* key, size_t key_len,
    int64_t timestamp, void* opaque,
    RdKafka::Headers* headers);

  std::string Name();
  Callbacks::Partitioner m_partitioner_cb;
  void ActivateDispatchers();
  void DeactivateDispatchers();

  void ConfigureCallback(Napi::Env env, const std::string &string_key, const Napi::Function &cb, bool add) override;
  Producer(Conf*, Conf*);
  ~Producer();
 protected:

 private:


  Callbacks::Delivery m_dr_cb;
  
};

}  // namespace NodeKafka

#endif  // SRC_PRODUCER_H_
