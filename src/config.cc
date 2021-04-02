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
#include <list>

#include "src/config.h"

using Napi::Object;
using std::cout;
using std::endl;

namespace NodeKafka {

void Conf::DumpConfig(std::list<std::string> *dump) {
  for (std::list<std::string>::iterator it = dump->begin();
         it != dump->end(); ) {
    std::cout << *it << " = ";
    it++;
    std::cout << *it << std::endl;
    it++;
  }
  std::cout << std::endl;
}

Conf * Conf::create(RdKafka::Conf::ConfType type, Napi::Object object, std::string &errstr) {  // NOLINT
  Conf* rdconf = static_cast<Conf*>(RdKafka::Conf::create(type));

  Napi::Array property_names = object.GetPropertyNames();

  for (unsigned int i = 0; i < property_names.Length(); ++i) {
    std::string string_value;
    std::string string_key;

    Napi::Value key = property_names.Get(i);
    Napi::Value value = object.Get(key);

    if (key.IsString()) {
      string_key = key.ToString().Utf8Value();
    } else {
      continue;
    }

    if (!value.IsFunction()) {
#if NODE_MAJOR_VERSION > 6
      if (value.IsNumber()) {//TODO support int32
        string_value = std::to_string(
          value.ToNumber().Uint32Value());
      } else if (value.IsBoolean()) {
        const bool v = value.ToBoolean().Value();
        string_value = v ? "true" : "false";
      } else {
        string_value = value.As<Napi::String>().Utf8Value();
      }
#else
      string_value = value.As<Napi::String>().Utf8Value();
#endif
      if (rdconf->set(string_key, string_value, errstr)
        != Conf::CONF_OK) {
          delete rdconf;
          return NULL;
      }
    } else {
      Napi::Function cb = value.As<Napi::Function>();
      rdconf->ConfigureCallback(string_key, cb, true, errstr);
      if (!errstr.empty()) {
        delete rdconf;
        return NULL;
      }
      rdconf->ConfigureCallback(string_key, cb, false, errstr);
      if (!errstr.empty()) {
        delete rdconf;
        return NULL;
      }
    }
  }

  return rdconf;
}

void Conf::ConfigureCallback(const std::string &string_key, const Napi::Function &cb, bool add, std::string &errstr) {
  if (string_key.compare("rebalance_cb") == 0) {
    if (add) {
      if (this->m_rebalance_cb == NULL) {
        this->m_rebalance_cb = new NodeKafka::Callbacks::Rebalance();
      }
      this->m_rebalance_cb->dispatcher.AddCallback(cb);
      this->set(string_key, this->m_rebalance_cb, errstr);
    } else {
      if (this->m_rebalance_cb != NULL) {
        this->m_rebalance_cb->dispatcher.RemoveCallback(cb);
      }
    }
  } else if (string_key.compare("offset_commit_cb") == 0) {
    if (add) {
      if (this->m_offset_commit_cb == NULL) {
        this->m_offset_commit_cb = new NodeKafka::Callbacks::OffsetCommit();
      }
      this->m_offset_commit_cb->dispatcher.AddCallback(cb);
      this->set(string_key, this->m_offset_commit_cb, errstr);
    } else {
      if (this->m_offset_commit_cb != NULL) {
        this->m_offset_commit_cb->dispatcher.RemoveCallback(cb);
      }
    }
  }
}

void Conf::listen() {
  if (m_rebalance_cb) {
    m_rebalance_cb->dispatcher.Activate();
  }

  if (m_offset_commit_cb) {
    m_offset_commit_cb->dispatcher.Activate();
  }
}

void Conf::stop() {
  if (m_rebalance_cb) {
    m_rebalance_cb->dispatcher.Deactivate();
  }

  if (m_offset_commit_cb) {
    m_offset_commit_cb->dispatcher.Deactivate();
  }
}

Conf::~Conf() {
  if (m_rebalance_cb) {
    delete m_rebalance_cb;
  }
}

}  // namespace NodeKafka
