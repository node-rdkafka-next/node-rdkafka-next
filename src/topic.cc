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

#include "src/common.h"
#include "src/connection.h"
#include "src/topic.h"

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

Topic::Topic(std::string topic_name, RdKafka::Conf* config):
  m_topic_name(topic_name),
  m_config(config) {
  // We probably want to copy the config. May require refactoring if we do not
}

Topic::~Topic() {
  if (m_config) {
    delete m_config;
  }
}

std::string Topic::name() {
  return m_topic_name;
}

Baton Topic::toRDKafkaTopic(Connection* handle) {
  if (m_config) {
    return handle->CreateTopic(m_topic_name, m_config);
  } else {
    return handle->CreateTopic(m_topic_name);
  }
}

/*

bool partition_available(int32_t partition) {
  return topic_->partition_available(partition);
}

Baton offset_store (int32_t partition, int64_t offset) {
  RdKafka::ErrorCode err = topic_->offset_store(partition, offset);

  switch (err) {
    case RdKafka::ERR_NO_ERROR:

      break;
    default:

      break;
  }
}

*/

}  // namespace NodeKafka
