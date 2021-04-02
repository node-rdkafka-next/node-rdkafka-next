/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_

#include <napi.h>
#include <uv.h>
#include <iostream>
#include <string>
#include <vector>

#include "rdkafkacpp.h"
#include "rdkafka.h" // NOLINT

#include "src/errors.h"

typedef std::vector<const RdKafka::BrokerMetadata *> BrokerMetadataList;
typedef std::vector<const RdKafka::PartitionMetadata *> PartitionMetadataList;
typedef std::vector<const RdKafka::TopicMetadata *> TopicMetadataList;

namespace NodeKafka
{

  void Log(std::string);

  template <typename T>
  T GetParameter(Napi::Object, std::string, T);
  template <>
  std::string GetParameter<std::string>(
      Napi::Object, std::string, std::string);
  template <>
  std::vector<std::string> GetParameter<std::vector<std::string>>(
      Napi::Object, std::string, std::vector<std::string>);
  // template int GetParameter<int>(v8::Local<Napi::Object, std::string, int);
  std::vector<std::string> v8ArrayToStringVector(Napi::Array);

  class scoped_mutex_lock
  {
  public:
    explicit scoped_mutex_lock(uv_mutex_t &lock_) : // NOLINT
                                                    async_lock(lock_)
    {
      uv_mutex_lock(&async_lock);
    }

    ~scoped_mutex_lock()
    {
      uv_mutex_unlock(&async_lock);
    }

  private:
    uv_mutex_t &async_lock;
  };

  /*
int uv_rwlock_tryrdlock(uv_rwlock_t* rwlock)

int uv_rwlock_trywrlock(uv_rwlock_t* rwlock)
 */

  class scoped_shared_write_lock
  {
  public:
    explicit scoped_shared_write_lock(uv_rwlock_t &lock_) : // NOLINT
                                                            async_lock(lock_)
    {
      uv_rwlock_wrlock(&async_lock);
    }

    ~scoped_shared_write_lock()
    {
      uv_rwlock_wrunlock(&async_lock);
    }

  private:
    uv_rwlock_t &async_lock;
  };

  class scoped_shared_read_lock
  {
  public:
    explicit scoped_shared_read_lock(uv_rwlock_t &lock_) : // NOLINT
                                                           async_lock(lock_)
    {
      uv_rwlock_rdlock(&async_lock);
    }

    ~scoped_shared_read_lock()
    {
      uv_rwlock_rdunlock(&async_lock);
    }

  private:
    uv_rwlock_t &async_lock;
  };

  namespace Conversion
  {

    namespace Admin
    {
      // Topics from topic object, or topic object array
      rd_kafka_NewTopic_t *FromV8TopicObject(
          Napi::Object, std::string &errstr); // NOLINT
      rd_kafka_NewTopic_t **FromV8TopicObjectArray(Napi::Array);
    }

    namespace Topic
    {
      std::vector<std::string> ToStringVector(Napi::Array);
      Napi::Array ToV8Array(Napi::Env env,std::vector<std::string>);
    } // namespace Topic

    namespace TopicPartition
    {

      Napi::Array ToV8Array(Napi::Env env, std::vector<RdKafka::TopicPartition *> &);
      RdKafka::TopicPartition *FromV8Object(Napi::Object);
      std::vector<RdKafka::TopicPartition *> FromV8Array(const Napi::Array &); // NOLINT

    } // namespace TopicPartition

    namespace Metadata
    {

      Napi::Object ToV8Object(Napi::Env, RdKafka::Metadata *);

    } // namespace Metadata

    namespace Message
    {

      Napi::Object ToV8Object(Napi::Env, RdKafka::Message *);
      Napi::Object ToV8Object(Napi::Env, RdKafka::Message *, bool, bool);

    }

  } // namespace Conversion

  namespace Util
  {
    std::string FromV8String(Napi::String);
  }

} // namespace NodeKafka

#endif // SRC_COMMON_H_
