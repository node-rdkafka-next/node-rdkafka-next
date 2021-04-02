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

#include "src/kafka-consumer.h"
#include "src/workers.h"


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

KafkaConsumer::KafkaConsumer(Conf* gconfig, Conf* tconfig):
  Connection(gconfig, tconfig) {
    std::string errstr;

    m_gconfig->set("default_topic_conf", m_tconfig, errstr);
  }

KafkaConsumer::~KafkaConsumer() {
  // We only want to run this if it hasn't been run already
  Disconnect();
}

Baton KafkaConsumer::Connect() {
  if (IsConnected()) {
    return Baton(RdKafka::ERR_NO_ERROR);
  }

  std::string errstr;
  {
    scoped_shared_write_lock lock(m_connection_lock);
    m_client = RdKafka::KafkaConsumer::create(m_gconfig, errstr);
  }

  if (!m_client || !errstr.empty()) {
    return Baton(RdKafka::ERR__STATE, errstr);
  }

  if (m_partitions.size() > 0) {
    m_client->resume(m_partitions);
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

void KafkaConsumer::ActivateDispatchers() {
  // Listen to global config
  m_gconfig->listen();

  // Listen to non global config
  // tconfig->listen();

  // This should be refactored to config based management
  m_event_cb.dispatcher.Activate();
}

Baton KafkaConsumer::Disconnect() {
  // Only close client if it is connected
  RdKafka::ErrorCode err = RdKafka::ERR_NO_ERROR;

  if (IsConnected()) {
    m_is_closing = true;
    {
      scoped_shared_write_lock lock(m_connection_lock);

      RdKafka::KafkaConsumer* consumer =
        dynamic_cast<RdKafka::KafkaConsumer*>(m_client);
      err = consumer->close();

      delete m_client;
      m_client = NULL;
    }
  }

  m_is_closing = false;

  return Baton(err);
}

void KafkaConsumer::DeactivateDispatchers() {
  // Stop listening to the config dispatchers
  m_gconfig->stop();

  // Also this one
  m_event_cb.dispatcher.Deactivate();
}

bool KafkaConsumer::IsSubscribed() {
  if (!IsConnected()) {
    return false;
  }

  if (!m_is_subscribed) {
    return false;
  }

  return true;
}


bool KafkaConsumer::HasAssignedPartitions() {
  return !m_partitions.empty();
}

int KafkaConsumer::AssignedPartitionCount() {
  return m_partition_cnt;
}

Baton KafkaConsumer::GetWatermarkOffsets(
  std::string topic_name, int32_t partition,
  int64_t* low_offset, int64_t* high_offset) {
  // Check if we are connected first

  RdKafka::ErrorCode err;

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      // Always send true - we
      err = m_client->get_watermark_offsets(topic_name, partition,
        low_offset, high_offset);
    } else {
      err = RdKafka::ERR__STATE;
    }
  } else {
    err = RdKafka::ERR__STATE;
  }

  return Baton(err);
}

void KafkaConsumer::part_list_print(const std::vector<RdKafka::TopicPartition*> &partitions) {  // NOLINT
  for (unsigned int i = 0 ; i < partitions.size() ; i++)
    std::cerr << partitions[i]->topic() <<
      "[" << partitions[i]->partition() << "], ";
  std::cerr << std::endl;
}

Baton KafkaConsumer::Assign(std::vector<RdKafka::TopicPartition*> partitions) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is disconnected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode errcode = consumer->assign(partitions);

  if (errcode == RdKafka::ERR_NO_ERROR) {
    m_partition_cnt = partitions.size();
    m_partitions.swap(partitions);
  }

  // Destroy the partitions: Either we're using them (and partitions
  // is now our old vector), or we're not using it as there was an
  // error.
  RdKafka::TopicPartition::destroy(partitions);

  return Baton(errcode);
}

Baton KafkaConsumer::Unassign() {
  if (!IsClosing() && !IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode errcode = consumer->unassign();

  if (errcode != RdKafka::ERR_NO_ERROR) {
    return Baton(errcode);
  }

  // Destroy the old list of partitions since we are no longer using it
  RdKafka::TopicPartition::destroy(m_partitions);

  m_partition_cnt = 0;

  return Baton(RdKafka::ERR_NO_ERROR);
}

Baton KafkaConsumer::Commit(std::vector<RdKafka::TopicPartition*> toppars) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->commitAsync(toppars);

  return Baton(err);
}

Baton KafkaConsumer::Commit(RdKafka::TopicPartition * toppar) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  // Need to put topic in a vector for it to work
  std::vector<RdKafka::TopicPartition*> offsets = {toppar};
  RdKafka::ErrorCode err = consumer->commitAsync(offsets);

  return Baton(err);
}

Baton KafkaConsumer::Commit() {
  // sets an error message
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->commitAsync();

  return Baton(err);
}

// Synchronous commit events
Baton KafkaConsumer::CommitSync(std::vector<RdKafka::TopicPartition*> toppars) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->commitSync(toppars);
  // RdKafka::TopicPartition::destroy(toppars);

  return Baton(err);
}

Baton KafkaConsumer::CommitSync(RdKafka::TopicPartition * toppar) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  // Need to put topic in a vector for it to work
  std::vector<RdKafka::TopicPartition*> offsets = {toppar};
  RdKafka::ErrorCode err = consumer->commitSync(offsets);

  return Baton(err);
}

Baton KafkaConsumer::CommitSync() {
  // sets an error message
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->commitSync();

  return Baton(err);
}

Baton KafkaConsumer::Seek(const RdKafka::TopicPartition &partition, int timeout_ms) {  // NOLINT
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->seek(partition, timeout_ms);

  return Baton(err);
}

Baton KafkaConsumer::Committed(std::vector<RdKafka::TopicPartition*> &toppars,
  int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->committed(toppars, timeout_ms);

  return Baton(err);
}

Baton KafkaConsumer::Position(std::vector<RdKafka::TopicPartition*> &toppars) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode err = consumer->position(toppars);

  return Baton(err);
}

Baton KafkaConsumer::Subscription() {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE, "Consumer is not connected");
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  // Needs to be a pointer since we're returning it through the baton
  std::vector<std::string> * topics = new std::vector<std::string>;

  RdKafka::ErrorCode err = consumer->subscription(*topics);

  if (err == RdKafka::ErrorCode::ERR_NO_ERROR) {
    // Good to go
    return Baton(topics);
  }

  return Baton(err);
}

Baton KafkaConsumer::Unsubscribe() {
  if (IsConnected() && IsSubscribed()) {
    RdKafka::KafkaConsumer* consumer =
      dynamic_cast<RdKafka::KafkaConsumer*>(m_client);
    consumer->unsubscribe();
    m_is_subscribed = false;
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

Baton KafkaConsumer::Pause(std::vector<RdKafka::TopicPartition*> & toppars) {
  if (IsConnected()) {
    RdKafka::KafkaConsumer* consumer =
      dynamic_cast<RdKafka::KafkaConsumer*>(m_client);
    RdKafka::ErrorCode err = consumer->pause(toppars);

    return Baton(err);
  }

  return Baton(RdKafka::ERR__STATE);
}

Baton KafkaConsumer::Resume(std::vector<RdKafka::TopicPartition*> & toppars) {
  if (IsConnected()) {
    RdKafka::KafkaConsumer* consumer =
      dynamic_cast<RdKafka::KafkaConsumer*>(m_client);
    RdKafka::ErrorCode err = consumer->resume(toppars);

    return Baton(err);
  }

  return Baton(RdKafka::ERR__STATE);
}

Baton KafkaConsumer::OffsetsStore(std::vector<RdKafka::TopicPartition*> & toppars) {  // NOLINT
  if (IsConnected() && IsSubscribed()) {
    RdKafka::KafkaConsumer* consumer =
      dynamic_cast<RdKafka::KafkaConsumer*>(m_client);
    RdKafka::ErrorCode err = consumer->offsets_store(toppars);

    return Baton(err);
  }

  return Baton(RdKafka::ERR__STATE);
}

Baton KafkaConsumer::Subscribe(std::vector<std::string> topics) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  RdKafka::ErrorCode errcode = consumer->subscribe(topics);
  if (errcode != RdKafka::ERR_NO_ERROR) {
    return Baton(errcode);
  }

  m_is_subscribed = true;

  return Baton(RdKafka::ERR_NO_ERROR);
}

Baton KafkaConsumer::Consume(int timeout_ms) {
  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
    } else {
      RdKafka::KafkaConsumer* consumer =
        dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

      RdKafka::Message * message = consumer->consume(timeout_ms);
      RdKafka::ErrorCode response_code = message->err();
      // we want to handle these errors at the call site
      if (response_code != RdKafka::ERR_NO_ERROR && 
         response_code != RdKafka::ERR__PARTITION_EOF &&
         response_code != RdKafka::ERR__TIMED_OUT &&
         response_code != RdKafka::ERR__TIMED_OUT_QUEUE
       ) {
        delete message;
        return Baton(response_code);
      }

      return Baton(message);
    }
  } else {
    return Baton(RdKafka::ERR__STATE, "KafkaConsumer is not connected");
  }
}

Baton KafkaConsumer::RefreshAssignments() {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  RdKafka::KafkaConsumer* consumer =
    dynamic_cast<RdKafka::KafkaConsumer*>(m_client);

  std::vector<RdKafka::TopicPartition*> partition_list;
  RdKafka::ErrorCode err = consumer->assignment(partition_list);

  switch (err) {
    case RdKafka::ERR_NO_ERROR:
      m_partition_cnt = partition_list.size();
      m_partitions.swap(partition_list);

      // These are pointers so we need to delete them somewhere.
      // Do it here because we're only going to convert when we're ready
      // to return to v8.
      RdKafka::TopicPartition::destroy(partition_list);
      return Baton(RdKafka::ERR_NO_ERROR);
    break;
    default:
      return Baton(err);
    break;
  }
}

std::string KafkaConsumer::Name() {
  if (!IsConnected()) {
    return std::string("");
  }
  return std::string(m_client->name());
}



}  // namespace NodeKafka
