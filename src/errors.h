/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_ERRORS_H_
#define SRC_ERRORS_H_

#include <napi.h>
#include <iostream>
#include <string>

#include "rdkafkacpp.h"

#include "src/common.h"

namespace NodeKafka {

class Baton {
 public:
  explicit Baton(const RdKafka::ErrorCode &);
  explicit Baton(void* data);
  explicit Baton(const RdKafka::ErrorCode &, std::string);

  template<typename T> T data() {
    return static_cast<T>(m_data);
  }

  RdKafka::ErrorCode err();
  std::string errstr();

  Napi::Object ToObject(Napi::Env);

 private:
  void* m_data;
  std::string m_errstr;
  RdKafka::ErrorCode m_err;
};

Napi::Object RdKafkaError(Napi::Env, const RdKafka::ErrorCode &);

}  // namespace NodeKafka

#endif  // SRC_ERRORS_H_
