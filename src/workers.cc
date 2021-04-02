/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <string>
#include <vector>

#include "src/workers.h"

#ifndef _WIN32
#include <unistd.h>
#else
// Windows specific
#include <time.h>
#endif

using NodeKafka::Producer;
using NodeKafka::Connection;

namespace NodeKafka {
namespace Workers {

namespace Handle {
/**
 * @brief Handle: get offsets for times.
 *
 * This callback will take a topic partition list with timestamps and
 * for each topic partition, will fill in the offsets. It is done async
 * because it has a timeout and I don't want node to block
 *
 * @see RdKafka::KafkaConsumer::Committed
 */

OffsetsForTimes::OffsetsForTimes(Napi::Function& callback,
                                 Connection* handle,
                                 std::vector<RdKafka::TopicPartition*> & t,
                                 const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_handle(handle),
  m_topic_partitions(t),
  m_timeout_ms(timeout_ms) {}

OffsetsForTimes::~OffsetsForTimes() {
  // Delete the underlying topic partitions as they are ephemeral or cloned
  RdKafka::TopicPartition::destroy(m_topic_partitions);
}

void OffsetsForTimes::Execute() {
  Baton b = m_handle->OffsetsForTimes(m_topic_partitions, m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void OffsetsForTimes::OnOk() {
  

  // const unsigned int argc = 2;
  // Napi::Value argv[argc];

  Callback().Call({Env().Null(), Conversion::TopicPartition::ToV8Array(Env(), m_topic_partitions)});
}


}  // namespace Handle

ConnectionMetadata::ConnectionMetadata(
  Napi::Function& callback, Connection* connection,
  std::string topic, int timeout_ms, bool all_topics) :
  ErrorAwareWorker(callback),
  m_connection(connection),
  m_topic(topic),
  m_timeout_ms(timeout_ms),
  m_all_topics(all_topics),
  m_metadata(NULL) {}

ConnectionMetadata::~ConnectionMetadata() {}

void ConnectionMetadata::Execute() {
  Baton b = m_connection->GetMetadata(m_all_topics, m_topic, m_timeout_ms);

  if (b.err() == RdKafka::ERR_NO_ERROR) {
    // No good way to do this except some stupid string delimiting.
    // maybe we'll delimit it by a | or something and just split
    // the string to create the object
    m_metadata = b.data<RdKafka::Metadata*>();
  } else {
    SetErrorBaton(b);
  }
}

void ConnectionMetadata::OnOk() {


  Callback().Call({ Env().Null(),
    Conversion::Metadata::ToV8Object(Env(), m_metadata)});

  delete m_metadata;
}



/**
 * @brief Client query watermark offsets worker
 *
 * Easy Napi::AsyncWorker for getting watermark offsets from a broker
 *
 * @sa RdKafka::Handle::query_watermark_offsets
 * @sa NodeKafka::Connection::QueryWatermarkOffsets
 */

ConnectionQueryWatermarkOffsets::ConnectionQueryWatermarkOffsets(
  Napi::Function& callback, Connection* connection,
  std::string topic, int32_t partition, int timeout_ms) :
  ErrorAwareWorker(callback),
  m_connection(connection),
  m_topic(topic),
  m_partition(partition),
  m_timeout_ms(timeout_ms) {}

ConnectionQueryWatermarkOffsets::~ConnectionQueryWatermarkOffsets() {}

void ConnectionQueryWatermarkOffsets::Execute() {
  Baton b = m_connection->QueryWatermarkOffsets(
    m_topic, m_partition, &m_low_offset, &m_high_offset, m_timeout_ms);

  // If we got any error here we need to bail out
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void ConnectionQueryWatermarkOffsets::OnOk() {

  Napi::Object offsetsObj = Napi::Object::New(Env());
  offsetsObj.Set("lowOffset",
  Napi::Number::New(Env(), m_low_offset));
  offsetsObj.Set("highOffset",
  Napi::Number::New(Env(), m_high_offset));

  // This is a big one!

  Callback().Call({ Env().Null(), offsetsObj});
}



/**
 * @brief Producer connect worker.
 *
 * Easy Napi::AsyncWorker for setting up client connections
 *
 * @sa RdKafka::Producer::connect
 * @sa NodeKafka::Producer::Connect
 */

ProducerConnect::ProducerConnect(Napi::Function& callback, Producer* producer):
  ErrorAwareWorker(callback),
  producer(producer) {}

ProducerConnect::~ProducerConnect() {}

void ProducerConnect::Execute() {
  Baton b = producer->Connect();

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void ProducerConnect::OnOk() {
  Napi::Object obj = Napi::Object::New(Env());
  obj.Set("name",
    Napi::String::New(Env(), producer->Name()));


  // Activate the dispatchers
  producer->ActivateDispatchers();

  Callback().Call({ Env().Null(), obj});
}



/**
 * @brief Producer disconnect worker
 *
 * Easy Napi::AsyncWorker for disconnecting from clients
 */

ProducerDisconnect::ProducerDisconnect(Napi::Function& callback,
  Producer* producer):
  ErrorAwareWorker(callback),
  producer(producer) {}

ProducerDisconnect::~ProducerDisconnect() {}

void ProducerDisconnect::Execute() {
  producer->Disconnect();
}

void ProducerDisconnect::OnOk() {
  // Deactivate the dispatchers
  producer->DeactivateDispatchers();

  Callback().Call({ Env().Null(), Napi::Boolean::New(Env(), true)});
}



/**
 * @brief Producer flush worker
 *
 * Easy Napi::AsyncWorker for flushing a producer.
 */

ProducerFlush::ProducerFlush(Napi::Function& callback,
  Producer* producer, int timeout_ms):
  ErrorAwareWorker(callback),
  producer(producer),
  timeout_ms(timeout_ms) {}

ProducerFlush::~ProducerFlush() {}

void ProducerFlush::Execute() {
  if (!producer->IsConnected()) {
    SetError("Producer is disconnected");
    return;
  }

  Baton b = producer->Flush(timeout_ms);
  if (b.err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void ProducerFlush::OnOk() {
  Callback().Call({ Env().Null() });
}

/**
 * @brief KafkaConsumer connect worker.
 *
 * Easy Napi::AsyncWorker for setting up client connections
 *
 * @sa RdKafka::KafkaConsumer::connect
 * @sa NodeKafka::KafkaConsumer::Connect
 */

KafkaConsumerConnect::KafkaConsumerConnect(Napi::Function& callback,
  KafkaConsumer* consumer):
  ErrorAwareWorker(callback),
  consumer(consumer) {}

KafkaConsumerConnect::~KafkaConsumerConnect() {}

void KafkaConsumerConnect::Execute() {
  Baton b = consumer->Connect();
  // consumer->Wait();

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void KafkaConsumerConnect::OnOk() {
  

  // Create the object
  Napi::Object obj = Napi::Object::New(Env());
  obj.Set("name",
    Napi::String::New(Env(), consumer->Name()));

  consumer->ActivateDispatchers();

  Callback().Call({ Env().Null(), obj });
}



/**
 * @brief KafkaConsumer disconnect worker.
 *
 * Easy Napi::AsyncWorker for disconnecting and cleaning up librdkafka artifacts
 *
 * @sa RdKafka::KafkaConsumer::disconnect
 * @sa NodeKafka::KafkaConsumer::Disconnect
 */

KafkaConsumerDisconnect::KafkaConsumerDisconnect(Napi::Function& callback,
  KafkaConsumer* consumer):
  ErrorAwareWorker(callback),
  consumer(consumer) {}

KafkaConsumerDisconnect::~KafkaConsumerDisconnect() {}

void KafkaConsumerDisconnect::Execute() {
  Baton b = consumer->Disconnect();

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void KafkaConsumerDisconnect::OnOk() {
  consumer->DeactivateDispatchers();

  Callback().Call({ Env().Null(), Napi::Boolean::New(Env(), true) });
}

void KafkaConsumerDisconnect::OnError(const Napi::Error& e) {
  consumer->DeactivateDispatchers();

  Callback().Call({Napi::String::New(Env(), e.Message())});
}

/**
 * @brief KafkaConsumer get messages worker.
 *
 * A more complex Nan::AsyncProgressWorker. I made a custom superclass to deal
 * with more real-time progress points. Instead of using ProgressWorker, which
 * is not time sensitive, this custom worker will poll using libuv and send
 * data back to v8 as it comes available without missing any
 *
 * The actual event runs through a continuous while loop. It stops when the
 * consumer is flagged as disconnected or as unsubscribed.
 *
 * @todo thread-safe isConnected checking
 * @note Chances are, when the connection is broken with the way librdkafka works,
 * we are shutting down. But we want it to shut down properly so we probably
 * need the consumer to have a thread lock that can be used when
 * we are dealing with manipulating the `client`
 *
 * @sa RdKafka::KafkaConsumer::Consume
 * @sa NodeKafka::KafkaConsumer::GetMessage
 */

KafkaConsumerConsumeLoop::KafkaConsumerConsumeLoop(Napi::Function& callback,
                                     KafkaConsumer* consumer,
                                     const int & timeout_ms,
                                     const int & timeout_sleep_delay_ms) :
  MessageWorker(callback),
  consumer(consumer),
  m_timeout_ms(timeout_ms),
  m_timeout_sleep_delay_ms(timeout_sleep_delay_ms),
  m_rand_seed(time(NULL)) {}

KafkaConsumerConsumeLoop::~KafkaConsumerConsumeLoop() {}

void KafkaConsumerConsumeLoop::Execute(const ExecutionMessageBus& bus) {
  // Do one check here before we move forward
  bool looping = true;
  while (consumer->IsConnected() && looping) {
    Baton b = consumer->Consume(m_timeout_ms);
    RdKafka::ErrorCode ec = b.err();
    if (ec == RdKafka::ERR_NO_ERROR) {
      RdKafka::Message *message = b.data<RdKafka::Message*>();
      switch (message->err()) {
        case RdKafka::ERR__PARTITION_EOF:
          bus.Send(message);
          // EOF means there are no more messages to read.
          // We should wait a little bit for more messages to come in
          // when in consume loop mode
          // Randomise the wait time to avoid contention on different
          // slow topics
          #ifndef _WIN32
          usleep(static_cast<int>(rand_r(&m_rand_seed) * 1000 * 1000 / RAND_MAX));
          #else
          _sleep(1000);
          #endif
          break;
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__TIMED_OUT_QUEUE:
          delete message;
          // If it is timed out this could just mean there were no
          // new messages fetched quickly enough. This isn't really
          // an error that should kill us.
          #ifndef _WIN32
          usleep(m_timeout_sleep_delay_ms*1000);
          #else
          _sleep(m_timeout_sleep_delay_ms);
          #endif
          break;
        case RdKafka::ERR_NO_ERROR:
          bus.Send(message);
          break;
        default:
          // Unknown error. We need to break out of this
          SetErrorBaton(b);
          looping = false;
          break;
        }
    } else if (ec == RdKafka::ERR_UNKNOWN_TOPIC_OR_PART || ec == RdKafka::ERR_TOPIC_AUTHORIZATION_FAILED) {
      bus.SendWarning(ec);
    } else {
      // Unknown error. We need to break out of this
      SetErrorBaton(b);
      looping = false;
    }
  }
}

void KafkaConsumerConsumeLoop::HandleMessageCallback(RdKafka::Message* msg, RdKafka::ErrorCode ec) {
  

  const unsigned int argc = 4;
  Napi::Value argv[argc];

  argv[0] = Env().Null();
  if (msg == NULL) {
    argv[1] = Env().Null();
    argv[2] = Env().Null();
    argv[3] = Napi::Number::New(Env(), ec);
  } else {
    argv[3] = Env().Null();
    switch (msg->err()) {
      case RdKafka::ERR__PARTITION_EOF: {
        argv[1] = Env().Null();
        Napi::Object eofEvent = Napi::Object::New(Env());

        eofEvent.Set("topic",
          Napi::String::New(Env(), msg->topic_name()));
        eofEvent.Set("offset",
          Napi::Number::New(Env(), msg->offset()));
        eofEvent.Set("partition",
          Napi::Number::New(Env(), msg->partition()));

        argv[2] = eofEvent;
        break;
      }
      default:
        argv[1] = Conversion::Message::ToV8Object(Env(), msg);
        argv[2] = Env().Null();
        break;
    }

    // We can delete msg now
    delete msg;
  }

  Callback().Call({argv[0], argv[1], argv[2], argv[3]});
}

void KafkaConsumerConsumeLoop::OnOk() {

}



/**
 * @brief KafkaConsumer get messages worker.
 *
 * This callback will get a number of messages. Can be of use in streams or
 * places where you don't want an infinite loop managed in C++land and would
 * rather manage it in Node.
 *
 * @see RdKafka::KafkaConsumer::Consume
 * @see NodeKafka::KafkaConsumer::GetMessage
 */

KafkaConsumerConsumeNum::KafkaConsumerConsumeNum(Napi::Function& callback,
                                     KafkaConsumer* consumer,
                                     const uint32_t & num_messages,
                                     const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_consumer(consumer),
  m_num_messages(num_messages),
  m_timeout_ms(timeout_ms) {}

KafkaConsumerConsumeNum::~KafkaConsumerConsumeNum() {}

void KafkaConsumerConsumeNum::Execute() {
  std::size_t max = static_cast<std::size_t>(m_num_messages);
  bool looping = true;
  int timeout_ms = m_timeout_ms;
  std::size_t eof_event_count = 0;

  while (m_messages.size() - eof_event_count < max && looping) {
    // Get a message
    Baton b = m_consumer->Consume(timeout_ms);
    if (b.err() == RdKafka::ERR_NO_ERROR) {
      RdKafka::Message *message = b.data<RdKafka::Message*>();
      RdKafka::ErrorCode errorCode = message->err();
      switch (errorCode) {
        case RdKafka::ERR__PARTITION_EOF:
          // If partition EOF and have consumed messages, retry with timeout 1
          // This allows getting ready messages, while not waiting for new ones
          if (m_messages.size() > eof_event_count) {
            timeout_ms = 1;
          }
          
          // We will only go into this code path when `enable.partition.eof` is set to true
          // In this case, consumer is also interested in EOF messages, so we return an EOF message
          m_messages.push_back(message);
          eof_event_count += 1;
          break;
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__TIMED_OUT_QUEUE:
          // Break of the loop if we timed out
          delete message;
          looping = false;
          break;
        case RdKafka::ERR_NO_ERROR:
          m_messages.push_back(b.data<RdKafka::Message*>());
          break;
        default:
          // Set the error for any other errors and break
          delete message;
          if (m_messages.size() == 0) {
            SetErrorBaton(Baton(errorCode));
          }
          looping = false;
          break;
      }
    } else {
      if (m_messages.size() == 0) {
        SetErrorBaton(b);
      }
      looping = false;
    }
  }
}

void KafkaConsumerConsumeNum::OnOk() {
  Napi::Array returnArray = Napi::Array::New(Env());
  Napi::Array eofEventsArray = Napi::Array::New(Env());

  if (m_messages.size() > 0) {
    int returnArrayIndex = -1;
    int eofEventsArrayIndex = -1;
    for (std::vector<RdKafka::Message*>::iterator it = m_messages.begin();
        it != m_messages.end(); ++it) {
      RdKafka::Message* message = *it;
      
      switch (message->err()) {
        case RdKafka::ERR_NO_ERROR:
          ++returnArrayIndex;
          returnArray.Set(returnArrayIndex, Conversion::Message::ToV8Object(Env(), message));
          break;
        case RdKafka::ERR__PARTITION_EOF:
          ++eofEventsArrayIndex;

          // create EOF event
          Napi::Object eofEvent = Napi::Object::New(Env());

          eofEvent.Set("topic",
            Napi::String::New(Env(), message->topic_name()));
          eofEvent.Set("offset",
            Napi::Number::New(Env(), message->offset()));
          eofEvent.Set("partition",
            Napi::Number::New(Env(), message->partition()));
          
          // also store index at which position in the message array this event was emitted
          // this way, we can later emit it at the right point in time
          eofEvent.Set("messageIndex",
            Napi::Number::New(Env(), returnArrayIndex));

          eofEventsArray.Set(eofEventsArrayIndex, eofEvent);
      }
      
      delete message;
    }
  }


  Callback().Call({Env().Null(), returnArray, eofEventsArray});
}

void KafkaConsumerConsumeNum::OnError(const Napi::Error& e) {

  if (m_messages.size() > 0) {
    for (std::vector<RdKafka::Message*>::iterator it = m_messages.begin();
        it != m_messages.end(); ++it) {
      RdKafka::Message* message = *it;
      delete message;
    }
  }

  Callback().Call({Napi::String::New(Env(), e.Message())});
}

/**
 * @brief KafkaConsumer get message worker.
 *
 * This callback will get a single message. Can be of use in streams or places
 * where you don't want an infinite loop managed in C++land and would rather
 * manage it in Node.
 *
 * @see RdKafka::KafkaConsumer::Consume
 * @see NodeKafka::KafkaConsumer::GetMessage
 */

KafkaConsumerConsume::KafkaConsumerConsume(Napi::Function& callback,
                                     KafkaConsumer* consumer,
                                     const int & timeout_ms) :
  ErrorAwareWorker(callback),
  consumer(consumer),
  m_timeout_ms(timeout_ms) {}

KafkaConsumerConsume::~KafkaConsumerConsume() {}

void KafkaConsumerConsume::Execute() {
  Baton b = consumer->Consume(m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  } else {
    RdKafka::Message *message = b.data<RdKafka::Message*>();
    RdKafka::ErrorCode errorCode = message->err();
    if (errorCode == RdKafka::ERR_NO_ERROR) {
      m_message = message;
    } else {
      delete message;
    }
  }
}

void KafkaConsumerConsume::OnOk() {

  delete m_message;

  Callback().Call({Env().Null(), Conversion::Message::ToV8Object(Env(), m_message)});
}



/**
 * @brief KafkaConsumer get committed topic partitions worker.
 *
 * This callback will get a topic partition list of committed offsets
 * for each topic partition. It is done async because it has a timeout
 * and I don't want node to block
 *
 * @see RdKafka::KafkaConsumer::Committed
 */

KafkaConsumerCommitted::KafkaConsumerCommitted(Napi::Function& callback,
                                     KafkaConsumer* consumer,
                                     std::vector<RdKafka::TopicPartition*> & t,
                                     const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_consumer(consumer),
  m_topic_partitions(t),
  m_timeout_ms(timeout_ms) {}

KafkaConsumerCommitted::~KafkaConsumerCommitted() {
  // Delete the underlying topic partitions as they are ephemeral or cloned
  RdKafka::TopicPartition::destroy(m_topic_partitions);
}

void KafkaConsumerCommitted::Execute() {
  Baton b = m_consumer->Committed(m_topic_partitions, m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void KafkaConsumerCommitted::OnOk() {

  Callback().Call({Env().Null(), Conversion::TopicPartition::ToV8Array(Env(), m_topic_partitions)});
}



/**
 * @brief KafkaConsumer seek
 *
 * This callback will take a topic partition list with offsets and
 * seek messages from there
 *
 * @see RdKafka::KafkaConsumer::seek
 *
 * @remark Consumption for the given partition must have started for the
 *         seek to work. Use assign() to set the starting offset.
 */

KafkaConsumerSeek::KafkaConsumerSeek(Napi::Function& callback,
                                     KafkaConsumer* consumer,
                                     const RdKafka::TopicPartition * toppar,
                                     const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_consumer(consumer),
  m_toppar(toppar),
  m_timeout_ms(timeout_ms) {}

KafkaConsumerSeek::~KafkaConsumerSeek() {
  if (m_timeout_ms > 0) {
    // Delete it when we are done with it.
    // However, if the timeout was less than 1, that means librdkafka is going
    // to queue the request up asynchronously, which apparently looks like if
    // we delete the memory here, since it was a pointer, librdkafka segfaults
    // when it actually does the operation (since it no longer blocks).

    // Well, that means we will be leaking memory when people do a timeout of 0
    // so... we should never get to this block. But just in case...
    delete m_toppar;
  }
}

void KafkaConsumerSeek::Execute() {
  Baton b = m_consumer->Seek(*m_toppar, m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void KafkaConsumerSeek::OnOk() {

  Callback().Call({Env().Null()});
}



/**
 * @brief createTopic
 *
 * This callback will create a topic
 *
 */
AdminClientCreateTopic::AdminClientCreateTopic(Napi::Function& callback,
                                               AdminClient* client,
                                               rd_kafka_NewTopic_t* topic,
                                               const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_client(client),
  m_topic(topic),
  m_timeout_ms(timeout_ms) {}

AdminClientCreateTopic::~AdminClientCreateTopic() {
  // Destroy the topic creation request when we are done
  rd_kafka_NewTopic_destroy(m_topic);
}

void AdminClientCreateTopic::Execute() {
  Baton b = m_client->CreateTopic(m_topic, m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void AdminClientCreateTopic::OnOk() {

  Callback().Call({Env().Null()});
}



/**
 * @brief Delete a topic in an asynchronous worker.
 *
 * This callback will delete a topic
 *
 */
AdminClientDeleteTopic::AdminClientDeleteTopic(Napi::Function& callback,
                                               AdminClient* client,
                                               rd_kafka_DeleteTopic_t* topic,
                                               const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_client(client),
  m_topic(topic),
  m_timeout_ms(timeout_ms) {}

AdminClientDeleteTopic::~AdminClientDeleteTopic() {
  // Destroy the topic creation request when we are done
  rd_kafka_DeleteTopic_destroy(m_topic);
}

void AdminClientDeleteTopic::Execute() {
  Baton b = m_client->DeleteTopic(m_topic, m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void AdminClientDeleteTopic::OnOk() {


  Callback().Call({Env().Null()});
}



/**
 * @brief Delete a topic in an asynchronous worker.
 *
 * This callback will delete a topic
 *
 */
AdminClientCreatePartitions::AdminClientCreatePartitions(
                                         Napi::Function& callback,
                                         AdminClient* client,
                                         rd_kafka_NewPartitions_t* partitions,
                                         const int & timeout_ms) :
  ErrorAwareWorker(callback),
  m_client(client),
  m_partitions(partitions),
  m_timeout_ms(timeout_ms) {}

AdminClientCreatePartitions::~AdminClientCreatePartitions() {
  // Destroy the topic creation request when we are done
  rd_kafka_NewPartitions_destroy(m_partitions);
}

void AdminClientCreatePartitions::Execute() {
  Baton b = m_client->CreatePartitions(m_partitions, m_timeout_ms);
  if (b.err() != RdKafka::ERR_NO_ERROR) {
    SetErrorBaton(b);
  }
}

void AdminClientCreatePartitions::OnOk() {

  Callback().Call({Env().Null()});
}



}  // namespace Workers
}  // namespace NodeKafka
