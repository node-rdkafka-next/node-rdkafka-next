/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <string>

#include "src/errors.h"

namespace NodeKafka {

Napi::Object RdKafkaError(Napi::Env env, const RdKafka::ErrorCode &err, std::string errstr) {  // NOLINT
  //
  int code = static_cast<int>(err);

  Napi::Object ret = Napi::Object::New(env);

  ret.Set("message",
    Napi::String::New(env, errstr));
  ret.Set("code",
    Napi::Number::New(env, code));

  return ret;
}

Napi::Object RdKafkaError(Napi::Env env, const RdKafka::ErrorCode &err) {
  return RdKafkaError(env, err, RdKafka::err2str(err));
}

Baton::Baton(const RdKafka::ErrorCode &code) {
  m_err = code;
}

Baton::Baton(const RdKafka::ErrorCode &code, std::string errstr) {
  m_err = code;
  m_errstr = errstr;
}

Baton::Baton(void* data) {
  m_err = RdKafka::ERR_NO_ERROR;
  m_data = data;
}

Napi::Object Baton::ToObject(Napi::Env env) {
  if (m_errstr.empty()) {
    return RdKafkaError(env, m_err);
  } else {
    return RdKafkaError(env, m_err, m_errstr);
  }
}

RdKafka::ErrorCode Baton::err() {
  return m_err;
}

std::string Baton::errstr() {
  if (m_errstr.empty()) {
    return RdKafka::err2str(m_err);
  } else {
    return m_errstr;
  }
}

}  // namespace NodeKafka
