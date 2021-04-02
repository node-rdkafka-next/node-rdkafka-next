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

#include "src/producer.h"
#include "src/workers.h"

namespace NodeKafka {

/**
 * @brief Producer v8 wrapped object.
 *
 * Wraps the RdKafka::Producer object with compositional inheritence and
 * provides methods for interacting with it exposed to node.
 *
 * The base wrappable RdKafka::Handle deals with most of the wrapping but
 * we still need to declare its prototype.
 *
 * @sa RdKafka::Producer
 * @sa NodeKafka::Connection
 */

Producer::Producer(Conf* gconfig, Conf* tconfig):
  Connection(gconfig, tconfig),
  m_dr_cb(),
  m_partitioner_cb() {
    std::string errstr;

    m_gconfig->set("default_topic_conf", m_tconfig, errstr);
    m_gconfig->set("dr_cb", &m_dr_cb, errstr);
  }

Producer::~Producer() {
  Disconnect();
}


std::string Producer::Name() {
  if (!IsConnected()) {
    return std::string("");
  }
  return std::string(m_client->name());
}

Baton Producer::Connect() {
  if (IsConnected()) {
    return Baton(RdKafka::ERR_NO_ERROR);
  }

  std::string errstr;
  {
    scoped_shared_read_lock lock(m_connection_lock);
    m_client = RdKafka::Producer::create(m_gconfig, errstr);
  }

  if (!m_client) {
    // @todo implement errstr into this somehow
    return Baton(RdKafka::ERR__STATE, errstr);
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

void Producer::ActivateDispatchers() {
  m_event_cb.dispatcher.Activate();  // From connection
  m_dr_cb.dispatcher.Activate();
}

void Producer::DeactivateDispatchers() {
  m_event_cb.dispatcher.Deactivate();  // From connection
  m_dr_cb.dispatcher.Deactivate();
}

void Producer::Disconnect() {
  if (IsConnected()) {
    scoped_shared_write_lock lock(m_connection_lock);
    delete m_client;
    m_client = NULL;
  }
}

/**
 * [Producer::Produce description]
 * @param message - pointer to the message we are sending. This method will
 * create a copy of it, so you are still required to free it when done.
 * @param size - size of the message. We are copying the memory so we need
 * the size
 * @param topic - RdKafka::Topic* object to send the message to. Generally
 * created by NodeKafka::Topic::toRDKafkaTopic
 * @param partition - partition to send it to. Send in
 * RdKafka::Topic::PARTITION_UA to send to an unassigned topic
 * @param key - a string pointer for the key, or null if there is none.
 * @return - A baton object with error code set if it failed.
 */
Baton Producer::Produce(void* message, size_t size, RdKafka::Topic* topic,
  int32_t partition, const void *key, size_t key_len, void* opaque) {
  RdKafka::ErrorCode response_code;

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      RdKafka::Producer* producer = dynamic_cast<RdKafka::Producer*>(m_client);
      response_code = producer->produce(topic, partition,
            RdKafka::Producer::RK_MSG_COPY,
            message, size, key, key_len, opaque);
    } else {
      response_code = RdKafka::ERR__STATE;
    }
  } else {
    response_code = RdKafka::ERR__STATE;
  }

  // These topics actually link to the configuration
  // they are made from. It's so we can reuse topic configurations
  // That means if we delete it here and librd thinks its still linked,
  // producing to the same topic will try to reuse it and it will die.
  //
  // Honestly, we may need to make configuration a first class object
  // @todo(Conf needs to be a first class object that is passed around)
  // delete topic;

  if (response_code != RdKafka::ERR_NO_ERROR) {
    return Baton(response_code);
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

/**
 * [Producer::Produce description]
 * @param message - pointer to the message we are sending. This method will
 * create a copy of it, so you are still required to free it when done.
 * @param size - size of the message. We are copying the memory so we need
 * the size
 * @param topic - String topic to use so we do not need to create
 * an RdKafka::Topic*
 * @param partition - partition to send it to. Send in
 * RdKafka::Topic::PARTITION_UA to send to an unassigned topic
 * @param key - a string pointer for the key, or null if there is none.
 * @return - A baton object with error code set if it failed.
 */
Baton Producer::Produce(void* message, size_t size, std::string topic,
  int32_t partition, std::string *key, int64_t timestamp, void* opaque,
  RdKafka::Headers* headers) {
  return Produce(message, size, topic, partition,
    key ? key->data() : NULL, key ? key->size() : 0,
    timestamp, opaque, headers);
}

/**
 * [Producer::Produce description]
 * @param message - pointer to the message we are sending. This method will
 * create a copy of it, so you are still required to free it when done.
 * @param size - size of the message. We are copying the memory so we need
 * the size
 * @param topic - String topic to use so we do not need to create
 * an RdKafka::Topic*
 * @param partition - partition to send it to. Send in
 * RdKafka::Topic::PARTITION_UA to send to an unassigned topic
 * @param key - a string pointer for the key, or null if there is none.
 * @return - A baton object with error code set if it failed.
 */
Baton Producer::Produce(void* message, size_t size, std::string topic,
  int32_t partition, const void *key, size_t key_len,
  int64_t timestamp, void* opaque, RdKafka::Headers* headers) {
  RdKafka::ErrorCode response_code;

  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      RdKafka::Producer* producer = dynamic_cast<RdKafka::Producer*>(m_client);
      // This one is a bit different
      response_code = producer->produce(topic, partition,
            RdKafka::Producer::RK_MSG_COPY,
            message, size,
            key, key_len,
            timestamp, headers, opaque);
    } else {
      response_code = RdKafka::ERR__STATE;
    }
  } else {
    response_code = RdKafka::ERR__STATE;
  }

  // These topics actually link to the configuration
  // they are made from. It's so we can reuse topic configurations
  // That means if we delete it here and librd thinks its still linked,
  // producing to the same topic will try to reuse it and it will die.
  //
  // Honestly, we may need to make configuration a first class object
  // @todo(Conf needs to be a first class object that is passed around)
  // delete topic;

  if (response_code != RdKafka::ERR_NO_ERROR) {
    return Baton(response_code);
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

void Producer::Poll() {
  m_client->poll(0);
}

void Producer::ConfigureCallback(Napi::Env env, const std::string &string_key, const Napi::Function &cb, bool add) {
  if (string_key.compare("delivery_cb") == 0) {
    if (add) {
      bool dr_msg_cb = false;
      Napi::String dr_msg_cb_key = Napi::String::New(env, "dr_msg_cb");
      if (cb.Has(dr_msg_cb_key)) {//TODO why frommaybe(false)
        Napi::Value v = cb.Get(dr_msg_cb_key);
        if (v.IsBoolean()) {
          dr_msg_cb = v.ToBoolean().Value();
        }
      }
      if (dr_msg_cb) {
        this->m_dr_cb.SendMessageBuffer(true);
      }
      this->m_dr_cb.dispatcher.AddCallback(cb);
    } else {
      this->m_dr_cb.dispatcher.RemoveCallback(cb);
    }
  } else {
    Connection::ConfigureCallback(env, string_key, cb, add);
  }
}


Baton Producer::Flush(int timeout_ms) {
  RdKafka::ErrorCode response_code;
  if (IsConnected()) {
    scoped_shared_read_lock lock(m_connection_lock);
    if (IsConnected()) {
      RdKafka::Producer* producer = dynamic_cast<RdKafka::Producer*>(m_client);
      response_code = producer->flush(timeout_ms);
    } else {
      response_code = RdKafka::ERR__STATE;
    }
  } else {
    response_code = RdKafka::ERR__STATE;
  }

  return Baton(response_code);
}


}  // namespace NodeKafka
