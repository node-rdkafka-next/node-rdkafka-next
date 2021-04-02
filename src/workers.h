/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_WORKERS_H_
#define SRC_WORKERS_H_

#include <uv.h>
#include <napi.h>
#include <string>
#include <vector>

#include "src/common.h"
#include "src/producer.h"
#include "src/kafka-consumer.h"
#include "src/admin.h"
#include "rdkafka.h"  // NOLINT

namespace NodeKafka {
namespace Workers {

class ErrorAwareWorker : public Napi::AsyncWorker {
 public:
  explicit ErrorAwareWorker(Napi::Function&  callback_) :
    Napi::AsyncWorker(callback_),
    m_baton(RdKafka::ERR_NO_ERROR) {}
  virtual ~ErrorAwareWorker() {}

  // virtual void Execute();
  // virtual void OnOK();
  /*
  void HandleErrorCallback() {

    // const unsigned int argc = 1;
    // Napi::Value argv[argc] = { Napi::Error::New(env, ErrorMessage()) };
    Callback().Call({
      Napi::String::New(Env(), m_baton.errstr().c_str())
      // Napi::Number::New(Env(), 11)
    });
    // callback->Call(argc, argv);
  }
  */

 protected:
  void SetErrorCode(const int & code) {
    RdKafka::ErrorCode rd_err = static_cast<RdKafka::ErrorCode>(code);
    SetErrorCode(rd_err);
  }
  void SetErrorCode(const RdKafka::ErrorCode & err) {
    SetErrorBaton(Baton(err));
  }
  void SetErrorBaton(const NodeKafka::Baton & baton) {
    m_baton = baton;
    // baton.errstr();

    SetError(m_baton.errstr());
  }

  int GetErrorCode() {
    return m_baton.err();
  }

  Napi::Object GetErrorObject() {
    return m_baton.ToObject(Env());
  }

  Baton m_baton;
};

class MessageWorker : public ErrorAwareWorker {
 public:
  explicit MessageWorker(Napi::Function&  callback_)
      : ErrorAwareWorker(callback_), m_asyncdata() {
    m_async = new uv_async_t;
    uv_async_init(
      uv_default_loop(),
      m_async,
      m_async_message);
    m_async->data = this;

    uv_mutex_init(&m_async_lock);
  }

  virtual ~MessageWorker() {
    uv_mutex_destroy(&m_async_lock);
  }

  void WorkMessage() {
    std::vector<RdKafka::Message*> message_queue;
    std::vector<RdKafka::ErrorCode> warning_queue;

    {
      scoped_mutex_lock lock(m_async_lock);
      // Copy the vector and empty it
      m_asyncdata.swap(message_queue);
      m_asyncwarning.swap(warning_queue);
    }

    for (unsigned int i = 0; i < message_queue.size(); i++) {
      HandleMessageCallback(message_queue[i], RdKafka::ERR_NO_ERROR);

      // we are done with it. it is about to go out of scope
      // for the last time so let's just free it up here. can't rely
      // on the destructor
    }

    for (unsigned int i = 0; i < warning_queue.size(); i++) {
      HandleMessageCallback(NULL, warning_queue[i]);
    }
  }

  class ExecutionMessageBus {
    friend class MessageWorker;
   public:
     void Send(RdKafka::Message* m) const {
       that_->Produce_(m);
     }
     void SendWarning(RdKafka::ErrorCode c) const {
       that_->ProduceWarning_(c);
     }
   private:
    explicit ExecutionMessageBus(MessageWorker* that) : that_(that) {}
    MessageWorker* const that_;
  };

  virtual void Execute(const ExecutionMessageBus&) = 0;
  virtual void HandleMessageCallback(RdKafka::Message*, RdKafka::ErrorCode) = 0;

  virtual void Destroy() {
    uv_close(reinterpret_cast<uv_handle_t*>(m_async), AsyncClose_);
  }

 private:
  void Execute() {
    ExecutionMessageBus message_bus(this);
    Execute(message_bus);
  }

  void Produce_(RdKafka::Message* m) {
    scoped_mutex_lock lock(m_async_lock);
    m_asyncdata.push_back(m);
    uv_async_send(m_async);
  }

  void ProduceWarning_(RdKafka::ErrorCode c) {
    scoped_mutex_lock lock(m_async_lock);
    m_asyncwarning.push_back(c);
    uv_async_send(m_async);
  }

  inline static void m_async_message(uv_async_t *async) {
    MessageWorker *worker = static_cast<MessageWorker*>(async->data);
    worker->WorkMessage();
  }

  inline static void AsyncClose_(uv_handle_t* handle) {
    MessageWorker *worker = static_cast<MessageWorker*>(handle->data);
    delete reinterpret_cast<uv_async_t*>(handle);
    delete worker;
  }

  uv_async_t *m_async;
  uv_mutex_t m_async_lock;
  std::vector<RdKafka::Message*> m_asyncdata;
  std::vector<RdKafka::ErrorCode> m_asyncwarning;
};

namespace Handle {
class OffsetsForTimes : public ErrorAwareWorker {
 public:
  OffsetsForTimes(Napi::Function& , NodeKafka::Connection*,
    std::vector<RdKafka::TopicPartition*> &,
    const int &);
  ~OffsetsForTimes();

  void Execute();
  void OnOk();

 private:
  NodeKafka::Connection * m_handle;
  std::vector<RdKafka::TopicPartition*> m_topic_partitions;
  const int m_timeout_ms;
};
}  // namespace Handle

class ConnectionMetadata : public ErrorAwareWorker {
 public:
  ConnectionMetadata(Napi::Function& , NodeKafka::Connection*,
    std::string, int, bool);
  ~ConnectionMetadata();

  void Execute();
  void OnOk();

 private:
  NodeKafka::Connection * m_connection;
  std::string m_topic;
  int m_timeout_ms;
  bool m_all_topics;

  RdKafka::Metadata* m_metadata;
};

class ConnectionQueryWatermarkOffsets : public ErrorAwareWorker {
 public:
  ConnectionQueryWatermarkOffsets(Napi::Function& , NodeKafka::Connection*,
    std::string, int32_t, int);
  ~ConnectionQueryWatermarkOffsets();

  void Execute();
  void OnOk();

 private:
  NodeKafka::Connection * m_connection;
  std::string m_topic;
  int32_t m_partition;
  int m_timeout_ms;

  int64_t m_high_offset;
  int64_t m_low_offset;
};

class ProducerConnect : public ErrorAwareWorker {
 public:
  ProducerConnect(Napi::Function& , NodeKafka::Producer*);
  ~ProducerConnect();

  void Execute();
  void OnOk();

 private:
  NodeKafka::Producer * producer;
};

class ProducerDisconnect : public ErrorAwareWorker {
 public:
  ProducerDisconnect(Napi::Function& , NodeKafka::Producer*);
  ~ProducerDisconnect();

  void Execute();
  void OnOk();

 private:
  NodeKafka::Producer * producer;
};

class ProducerFlush : public ErrorAwareWorker {
 public:
  ProducerFlush(Napi::Function& , NodeKafka::Producer*, int);
  ~ProducerFlush();

  void Execute();
  void OnOk();

 private:
  NodeKafka::Producer * producer;
  int timeout_ms;
};

class KafkaConsumerConnect : public ErrorAwareWorker {
 public:
  KafkaConsumerConnect(Napi::Function& , NodeKafka::KafkaConsumer*);
  ~KafkaConsumerConnect();

  void Execute();
  void OnOk();

 private:
  NodeKafka::KafkaConsumer * consumer;
};

class KafkaConsumerDisconnect : public ErrorAwareWorker {
 public:
  KafkaConsumerDisconnect(Napi::Function& , NodeKafka::KafkaConsumer*);
  ~KafkaConsumerDisconnect();

  void Execute();
  void OnOk();
  void OnError(const Napi::Error& );
 private:
  NodeKafka::KafkaConsumer * consumer;
};

class KafkaConsumerConsumeLoop : public MessageWorker {
 public:
  KafkaConsumerConsumeLoop(Napi::Function& ,
    NodeKafka::KafkaConsumer*, const int &, const int &);
  ~KafkaConsumerConsumeLoop();

  void Execute(const ExecutionMessageBus&);
  void OnOk();
  void HandleMessageCallback(RdKafka::Message*, RdKafka::ErrorCode);
 private:
  NodeKafka::KafkaConsumer * consumer;
  const int m_timeout_ms;
  unsigned int m_rand_seed;
  const int m_timeout_sleep_delay_ms;
};

class KafkaConsumerConsume : public ErrorAwareWorker {
 public:
  KafkaConsumerConsume(Napi::Function& , NodeKafka::KafkaConsumer*, const int &);
  ~KafkaConsumerConsume();

  void Execute();
  void OnOk();
 private:
  NodeKafka::KafkaConsumer * consumer;
  const int m_timeout_ms;
  RdKafka::Message* m_message;
};

class KafkaConsumerCommitted : public ErrorAwareWorker {
 public:
  KafkaConsumerCommitted(Napi::Function& ,
    NodeKafka::KafkaConsumer*, std::vector<RdKafka::TopicPartition*> &,
    const int &);
  ~KafkaConsumerCommitted();

  void Execute();
  void OnOk();
 private:
  NodeKafka::KafkaConsumer * m_consumer;
  std::vector<RdKafka::TopicPartition*> m_topic_partitions;
  const int m_timeout_ms;
};

class KafkaConsumerSeek : public ErrorAwareWorker {
 public:
  KafkaConsumerSeek(Napi::Function& , NodeKafka::KafkaConsumer*,
    const RdKafka::TopicPartition *, const int &);
  ~KafkaConsumerSeek();

  void Execute();
  void OnOk();
 private:
  NodeKafka::KafkaConsumer * m_consumer;
  const RdKafka::TopicPartition * m_toppar;
  const int m_timeout_ms;
};

class KafkaConsumerConsumeNum : public ErrorAwareWorker {
 public:
  KafkaConsumerConsumeNum(Napi::Function& , NodeKafka::KafkaConsumer*,
    const uint32_t &, const int &);
  ~KafkaConsumerConsumeNum();

  void Execute();
  void OnOk();
  void OnError(const Napi::Error& e);
 private:
  NodeKafka::KafkaConsumer * m_consumer;
  const uint32_t m_num_messages;
  const int m_timeout_ms;
  std::vector<RdKafka::Message*> m_messages;
};

/**
 * @brief Create a kafka topic on a remote broker cluster
 */
class AdminClientCreateTopic : public ErrorAwareWorker {
 public:
  AdminClientCreateTopic(Napi::Function& , NodeKafka::AdminClient*,
    rd_kafka_NewTopic_t*, const int &);
  ~AdminClientCreateTopic();

  void Execute();
  void OnOk();
 private:
  NodeKafka::AdminClient * m_client;
  rd_kafka_NewTopic_t* m_topic;
  const int m_timeout_ms;
};

/**
 * @brief Delete a kafka topic on a remote broker cluster
 */
class AdminClientDeleteTopic : public ErrorAwareWorker {
 public:
  AdminClientDeleteTopic(Napi::Function& , NodeKafka::AdminClient*,
    rd_kafka_DeleteTopic_t*, const int &);
  ~AdminClientDeleteTopic();

  void Execute();
  void OnOk();
 private:
  NodeKafka::AdminClient * m_client;
  rd_kafka_DeleteTopic_t* m_topic;
  const int m_timeout_ms;
};

/**
 * @brief Delete a kafka topic on a remote broker cluster
 */
class AdminClientCreatePartitions : public ErrorAwareWorker {
 public:
  AdminClientCreatePartitions(Napi::Function& , NodeKafka::AdminClient*,
    rd_kafka_NewPartitions_t*, const int &);
  ~AdminClientCreatePartitions();

  void Execute();
  void OnOk();
 private:
  NodeKafka::AdminClient * m_client;
  rd_kafka_NewPartitions_t* m_partitions;
  const int m_timeout_ms;
};

}  // namespace Workers

}  // namespace NodeKafka

#endif  // SRC_WORKERS_H_
