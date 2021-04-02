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
#include "src/admin.h"


namespace NodeKafka {

/**
 * @brief AdminClient v8 wrapped object.
 *
 * Specializes the connection to wrap a consumer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

AdminClient::AdminClient(Conf* gconfig):
  Connection(gconfig, NULL) {
    rkqu = NULL;
}

AdminClient::~AdminClient() {
  Disconnect();
}

Baton AdminClient::Connect() {
  std::string errstr;

  {
    scoped_shared_write_lock lock(m_connection_lock);
    m_client = RdKafka::Producer::create(m_gconfig, errstr);
  }

  if (!m_client || !errstr.empty()) {
    return Baton(RdKafka::ERR__STATE, errstr);
  }

  if (rkqu == NULL) {
    rkqu = rd_kafka_queue_new(m_client->c_ptr());
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

Baton AdminClient::Disconnect() {
  if (IsConnected()) {
    scoped_shared_write_lock lock(m_connection_lock);

    if (rkqu != NULL) {
      rd_kafka_queue_destroy(rkqu);
      rkqu = NULL;
    }

    delete m_client;
    m_client = NULL;
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}



/**
 * Poll for a particular event on a queue.
 *
 * This will keep polling until it gets an event of that type,
 * given the number of tries and a timeout
 */
rd_kafka_event_t* PollForEvent(
  rd_kafka_queue_t * topic_rkqu,
  rd_kafka_event_type_t event_type,
  int max_tries,
  int timeout_ms) {
  // Establish what attempt we are on
  int attempt = 0;

  rd_kafka_event_t * event_response = nullptr;

  // Poll the event queue until we get it
  do {
    // free previously fetched event
    rd_kafka_event_destroy(event_response);
    // Increment attempt counter
    attempt = attempt + 1;
    event_response = rd_kafka_queue_poll(topic_rkqu, timeout_ms);
  } while (
    rd_kafka_event_type(event_response) != event_type &&
    attempt < max_tries);

  // If this isn't the type of response we want, or if we do not have a response
  // type, bail out with a null
  if (event_response == NULL ||
    rd_kafka_event_type(event_response) != event_type) {
    rd_kafka_event_destroy(event_response);
    return NULL;
  }

  return event_response;
}

Baton AdminClient::CreateTopic(rd_kafka_NewTopic_t* topic, int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are creating topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
      m_client->c_ptr(), RD_KAFKA_ADMIN_OP_CREATETOPICS);

    // Create queue just for this operation
    rd_kafka_queue_t * topic_rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_CreateTopics(m_client->c_ptr(), &topic, 1, options, topic_rkqu);

    // Poll for an event by type in that queue
    rd_kafka_event_t * event_response = PollForEvent(
      topic_rkqu,
      RD_KAFKA_EVENT_CREATETOPICS_RESULT,
      5,
      1000);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(topic_rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(event_response);
      rd_kafka_event_destroy(event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // get the created results
    const rd_kafka_CreateTopics_result_t * create_topic_results =
      rd_kafka_event_CreateTopics_result(event_response);

    size_t created_topic_count;
    const rd_kafka_topic_result_t **restopics = rd_kafka_CreateTopics_result_topics(  // NOLINT
      create_topic_results,
      &created_topic_count);

    for (int i = 0 ; i < static_cast<int>(created_topic_count) ; i++) {
      const rd_kafka_topic_result_t *terr = restopics[i];
      const rd_kafka_resp_err_t errcode = rd_kafka_topic_result_error(terr);
      const char *errmsg = rd_kafka_topic_result_error_string(terr);

      if (errcode != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (errmsg) {
          const std::string errormsg = std::string(errmsg);
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode), errormsg); // NOLINT
        } else {
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode));
        }
      }
    }

    rd_kafka_event_destroy(event_response);
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::DeleteTopic(rd_kafka_DeleteTopic_t* topic, int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are deleting topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
      m_client->c_ptr(), RD_KAFKA_ADMIN_OP_DELETETOPICS);

    // Create queue just for this operation.
    // May be worth making a "scoped queue" class or something like a lock
    // for RAII
    rd_kafka_queue_t * topic_rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_DeleteTopics(m_client->c_ptr(), &topic, 1, options, topic_rkqu);

    // Poll for an event by type in that queue
    rd_kafka_event_t * event_response = PollForEvent(
      topic_rkqu,
      RD_KAFKA_EVENT_DELETETOPICS_RESULT,
      5,
      1000);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(topic_rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(event_response);
      rd_kafka_event_destroy(event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // get the created results
    const rd_kafka_DeleteTopics_result_t * delete_topic_results =
      rd_kafka_event_DeleteTopics_result(event_response);

    size_t deleted_topic_count;
    const rd_kafka_topic_result_t **restopics = rd_kafka_DeleteTopics_result_topics(  // NOLINT
      delete_topic_results,
      &deleted_topic_count);

    for (int i = 0 ; i < static_cast<int>(deleted_topic_count) ; i++) {
      const rd_kafka_topic_result_t *terr = restopics[i];
      const rd_kafka_resp_err_t errcode = rd_kafka_topic_result_error(terr);

      if (errcode != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_event_destroy(event_response);
        return Baton(static_cast<RdKafka::ErrorCode>(errcode));
      }
    }

    rd_kafka_event_destroy(event_response);
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::CreatePartitions(
  rd_kafka_NewPartitions_t* partitions,
  int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are deleting topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
      m_client->c_ptr(), RD_KAFKA_ADMIN_OP_CREATEPARTITIONS);

    // Create queue just for this operation.
    // May be worth making a "scoped queue" class or something like a lock
    // for RAII
    rd_kafka_queue_t * topic_rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_CreatePartitions(m_client->c_ptr(),
      &partitions, 1, options, topic_rkqu);

    // Poll for an event by type in that queue
    rd_kafka_event_t * event_response = PollForEvent(
      topic_rkqu,
      RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT,
      5,
      1000);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(topic_rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(event_response);
      rd_kafka_event_destroy(event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // get the created results
    const rd_kafka_CreatePartitions_result_t * create_partitions_results =
      rd_kafka_event_CreatePartitions_result(event_response);

    size_t created_partitions_topic_count;
    const rd_kafka_topic_result_t **restopics = rd_kafka_CreatePartitions_result_topics(  // NOLINT
      create_partitions_results,
      &created_partitions_topic_count);

    for (int i = 0 ; i < static_cast<int>(created_partitions_topic_count) ; i++) {  // NOLINT
      const rd_kafka_topic_result_t *terr = restopics[i];
      const rd_kafka_resp_err_t errcode = rd_kafka_topic_result_error(terr);
      const char *errmsg = rd_kafka_topic_result_error_string(terr);

      if (errcode != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (errmsg) {
          const std::string errormsg = std::string(errmsg);
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode), errormsg); // NOLINT
        } else {
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode));
        }
      }
    }

    rd_kafka_event_destroy(event_response);
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

void AdminClient::ActivateDispatchers() {
  // Listen to global config
  m_gconfig->listen();

  // Listen to non global config
  // tconfig->listen();

  // This should be refactored to config based management
  m_event_cb.dispatcher.Activate();
}
void AdminClient::DeactivateDispatchers() {
  // Stop listening to the config dispatchers
  m_gconfig->stop();

  // Also this one
  m_event_cb.dispatcher.Deactivate();
}


}  // namespace NodeKafka
