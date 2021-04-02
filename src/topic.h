/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_TOPIC_H_
#define SRC_TOPIC_H_

#include <napi.h>
#include <string>

#include "rdkafkacpp.h"

#include "src/config.h"

namespace NodeKafka {

class Topic {
 public:

  Baton toRDKafkaTopic(Connection *handle);
  Topic(std::string, RdKafka::Conf *);
  ~Topic();
  std::string name();
 protected:


  // TopicConfig * config_;

  std::string errstr;
  

 private:
  

  std::string m_topic_name;
  RdKafka::Conf * m_config;

};

}  // namespace NodeKafka

#endif  // SRC_TOPIC_H_
