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

namespace NodeKafka {

void Log(std::string str) { std::cerr << "% " << str.c_str() << std::endl; }

template <typename T>
T GetParameter(Napi::Object object, std::string field_name, T def) {

  return def;
}

template <>
int64_t GetParameter<int64_t>(Napi::Object object, std::string field_name,
                              int64_t def) {
  Napi::Value value = object.Get(field_name);

  if (!value.IsNumber()) {
    return def;
  }
  return value.ToNumber().Int64Value();
}

template <>
bool GetParameter<bool>(Napi::Object object, std::string field_name, bool def) {
  Napi::Value value = object.Get(field_name);
  if (!value.IsBoolean()) {
    return def;
  }
  return value.ToBoolean().Value();
}

template <>
int GetParameter<int>(Napi::Object object, std::string field_name, int def) {
  return static_cast<int>(GetParameter<int64_t>(object, field_name, def));
}

template <>
std::string GetParameter<std::string>(Napi::Object object,
                                      std::string field_name, std::string def) {
  Napi::Value value = object.Get(field_name);
  if (value.IsEmpty() || !value.IsString()) {
    return def;
  }
  return value.ToString();
}

template <>
std::vector<std::string> GetParameter<std::vector<std::string>>(
    Napi::Object object, std::string field_name, std::vector<std::string> def) {
  Napi::Value value = object.Get(field_name);
  if (value.IsEmpty() || !value.IsArray()) {
    return def;
  }
  Napi::Array array = value.As<Napi::Array>();
  return v8ArrayToStringVector(array);
}

std::vector<std::string> v8ArrayToStringVector(Napi::Array parameter) {
  std::vector<std::string> newItem;

  if (parameter.Length() >= 1) {
    for (unsigned int i = 0; i < parameter.Length(); i++) {
      newItem.push_back(parameter.Get(i).ToString().Utf8Value());
    }
  }
  return newItem;
}

namespace Conversion {
namespace Topic {

std::vector<std::string> ToStringVector(Napi::Array parameter) {
  std::vector<std::string> newItem;

  if (parameter.Length() >= 1) {
    for (unsigned int i = 0; i < parameter.Length(); i++) {
      // Napi::Value element;
      // if (!Nan::Get(parameter, i).ToLocal(&element)) {
      //   continue;
      // }
      Napi::Value element = parameter.Get(i);
      if (element.IsEmpty()) {
        continue;
      }

      // drop the support of regexp
      newItem.push_back(element.ToString().Utf8Value());
    }
  }

  return newItem;
}

Napi::Array ToV8Array(Napi::Env env, std::vector<std::string> parameter) {
  size_t size = parameter.size();
  Napi::Array newItem = Napi::Array::New(env, size);
  for (size_t i = 0; i < size; i++) {
    std::string topic = parameter[i];

    newItem.Set(i, topic);
  }

  return newItem;
}

} // namespace Topic

namespace TopicPartition {

/**
 * @brief RdKafka::TopicPartition vector to a v8 Array
 *
 * @see v8ArrayToTopicPartitionVector
 */
Napi::Array ToV8Array(
    Napi::Env env,
    std::vector<RdKafka::TopicPartition *> &topic_partition_list) { // NOLINT
  // Napi::Array array = Napi::Array::New(env);
  size_t size = topic_partition_list.size();
  Napi::Array array = Napi::Array::New(env, size);
  for (size_t topic_partition_i = 0; topic_partition_i < size;
       topic_partition_i++) {
    RdKafka::TopicPartition *topic_partition =
        topic_partition_list[topic_partition_i];

    if (topic_partition->err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
      array.Set(
          topic_partition_i,
          Napi::String::New(env, RdKafka::err2str(topic_partition->err())));
    } else {
      // We have the list now let's get the properties from it
      Napi::Object obj = Napi::Object::New(env);

      if (topic_partition->offset() != RdKafka::Topic::OFFSET_INVALID) {
        obj.Set("offset", Napi::Number::New(env, topic_partition->offset()));
      }
      obj.Set("partition",
              Napi::Number::New(env, topic_partition->partition()));
      obj.Set("topic",
              Napi::String::New(env, topic_partition->topic().c_str()));

      array.Set(topic_partition_i, obj);
    }
  }

  return array;
}

/**
 * @brief v8 Array of topic partitions to RdKafka::TopicPartition vector
 *
 * @see v8ArrayToTopicPartitionVector
 *
 * @note You must delete all the pointers inside here when you are done!!
 */
std::vector<RdKafka::TopicPartition *>
FromV8Array(const Napi::Array &topic_partition_list) {
  // NOTE: ARRAY OF POINTERS! DELETE THEM WHEN YOU ARE FINISHED
  std::vector<RdKafka::TopicPartition *> array;

  for (size_t topic_partition_i = 0;
       topic_partition_i < topic_partition_list.Length(); topic_partition_i++) {
    Napi::Value topic_partition_value =
        topic_partition_list.Get(topic_partition_i);
    if (topic_partition_value.IsEmpty()) {
      continue;
    }

    if (topic_partition_value.IsObject()) {
      array.push_back(FromV8Object(topic_partition_value.ToObject()));
    }
  }

  return array;
}

/**
 * @brief Napi::Object to RdKafka::TopicPartition
 *
 */
RdKafka::TopicPartition *FromV8Object(Napi::Object topic_partition) {
  std::string topic = GetParameter<std::string>(topic_partition, "topic", "");
  int partition = GetParameter<int>(topic_partition, "partition", -1);
  int64_t offset = GetParameter<int64_t>(topic_partition, "offset", 0);

  if (partition == -1) {
    return NULL;
  }

  if (topic.empty()) {
    return NULL;
  }

  return RdKafka::TopicPartition::create(topic, partition, offset);
}

} // namespace TopicPartition

namespace Metadata {

/**
 * @brief RdKafka::Metadata to Napi::Object
 *
 */
Napi::Object ToV8Object(Napi::Env env, RdKafka::Metadata *metadata) {
  Napi::Object obj = Napi::Object::New(env);

  Napi::Array broker_data = Napi::Array::New(env);
  Napi::Array topic_data = Napi::Array::New(env);

  const BrokerMetadataList *brokers = metadata->brokers(); // NOLINT

  unsigned int broker_i = 0;

  for (BrokerMetadataList::const_iterator it = brokers->begin();
       it != brokers->end(); ++it, broker_i++) {
    // Start iterating over brokers and set the object up

    const RdKafka::BrokerMetadata *x = *it;

    Napi::Object current_broker = Napi::Object::New(env);

    current_broker.Set("id", Napi::Number::New(env, x->id()));
    current_broker.Set("host", Napi::String::New(env, x->host().c_str()));
    current_broker.Set("port", Napi::Number::New(env, x->port()));

    broker_data.Set(broker_i, current_broker);
  }

  unsigned int topic_i = 0;

  const TopicMetadataList *topics = metadata->topics();

  for (TopicMetadataList::const_iterator it = topics->begin();
       it != topics->end(); ++it, topic_i++) {
    // Start iterating over topics

    const RdKafka::TopicMetadata *x = *it;

    Napi::Object current_topic = Napi::Object::New(env);

    current_topic.Set("name", Napi::String::New(env, x->topic().c_str()));

    Napi::Array current_topic_partitions = Napi::Array::New(env);

    const PartitionMetadataList *current_partition_data = x->partitions();

    unsigned int partition_i = 0;
    PartitionMetadataList::const_iterator itt;

    for (itt = current_partition_data->begin();
         itt != current_partition_data->end(); ++itt, partition_i++) {
      // partition iterate
      const RdKafka::PartitionMetadata *xx = *itt;

      Napi::Object current_partition = Napi::Object::New(env);

      current_partition.Set("id", Napi::Number::New(env, xx->id()));
      current_partition.Set("leader", Napi::Number::New(env, xx->leader()));

      const std::vector<int32_t> *replicas = xx->replicas();
      const std::vector<int32_t> *isrs = xx->isrs();

      std::vector<int32_t>::const_iterator r_it;
      std::vector<int32_t>::const_iterator i_it;

      unsigned int r_i = 0;
      unsigned int i_i = 0;

      Napi::Array current_replicas = Napi::Array::New(env);

      for (r_it = replicas->begin(); r_it != replicas->end(); ++r_it, r_i++) {
        current_replicas.Set(r_i, Napi::Number::New(env, *r_it));
      }

      Napi::Array current_isrs = Napi::Array::New(env);

      for (i_it = isrs->begin(); i_it != isrs->end(); ++i_it, i_i++) {
        current_isrs.Set(i_i, Napi::Number::New(env, *i_it));
      }

      current_partition.Set("replicas", current_replicas);
      current_partition.Set("isrs", current_isrs);

      current_topic_partitions.Set(partition_i, current_partition);
    } // iterate over partitions

    current_topic.Set("partitions", current_topic_partitions);

    topic_data.Set(topic_i, current_topic);
  } // End iterating over topics

  obj.Set("orig_broker_id", Napi::Number::New(env, metadata->orig_broker_id()));

  obj.Set("orig_broker_name",
          Napi::String::New(env, metadata->orig_broker_name()));

  obj.Set("topics", topic_data);
  obj.Set("brokers", broker_data);

  return obj;
}

} // namespace Metadata

namespace Message {

// Overload for all use cases except delivery reports
Napi::Object ToV8Object(Napi::Env env, RdKafka::Message *message) {
  return ToV8Object(env, message, true, true);
}

Napi::Object ToV8Object(Napi::Env env, RdKafka::Message *message,
                        bool include_payload, bool include_headers) {
  if (message->err() == RdKafka::ERR_NO_ERROR) {
    Napi::Object pack = Napi::Object::New(env);

    const void *message_payload = message->payload();
    if (!include_payload) {
      pack.Set("value", env.Undefined());
    } else if (message_payload) {
      pack.Set("value",
               Napi::Buffer<void>::New(env, const_cast<void *>(message_payload),
                                       message->len()));
    } else {
      pack.Set("value", env.Null());
    }

    RdKafka::Headers *headers;
    if (((headers = message->headers()) != 0) && include_headers) {
      Napi::Array v8headers = Napi::Array::New(env);
      int index = 0;
      std::vector<RdKafka::Headers::Header> all = headers->get_all();
      for (std::vector<RdKafka::Headers::Header>::iterator it = all.begin();
           it != all.end(); it++) {
        Napi::Object v8header = Napi::Object::New(env);
        v8header.Set(it->key(), Napi::Buffer<char>::New(
                                    env, const_cast<char *>(it->value_string()),
                                    it->value_size()));
        v8headers.Set(index, v8header);
        index++;
      }
      pack.Set("headers", v8headers);
    }

    pack.Set("size", Napi::Number::New(env, message->len()));

    const void *key_payload = message->key_pointer();

    if (key_payload) {
      // We want this to also be a buffer to avoid corruption
      // https://github.com/Blizzard/node-rdkafka/issues/208
      pack.Set("key",
               Napi::Buffer<void>::New(env, const_cast<void *>(key_payload),
                                       message->key_len()));
    } else {
      pack.Set("key", env.Null());
    }

    pack.Set("topic", Napi::String::New(env, message->topic_name()));
    pack.Set("offset", Napi::Number::New(env, message->offset()));
    pack.Set("partition", Napi::Number::New(env, message->partition()));
    pack.Set("timestamp",
             Napi::Number::New(env, message->timestamp().timestamp));

    return pack;
  } else {
    return RdKafkaError(env, message->err());
  }
}

} // namespace Message

/**
 * @section Admin API models
 */

namespace Admin {

/**
 * Create a low level rdkafka handle to represent a topic
 *
 *
 */
rd_kafka_NewTopic_t *FromV8TopicObject(Napi::Object object,
                                       std::string &errstr) { // NOLINT
  std::string topic_name = GetParameter<std::string>(object, "topic", "");
  int num_partitions = GetParameter<int>(object, "num_partitions", 0);
  int replication_factor = GetParameter<int>(object, "replication_factor", 0);

  // Too slow to allocate this every call but admin api
  // shouldn't be called that often
  char *errbuf = reinterpret_cast<char *>(malloc(100));
  size_t errstr_size = 100;

  rd_kafka_NewTopic_t *new_topic =
      rd_kafka_NewTopic_new(topic_name.c_str(), num_partitions,
                            replication_factor, errbuf, errstr_size);

  if (new_topic == NULL) {
    errstr = std::string(errbuf, errstr_size);
    free(errbuf);
    return NULL;
  }

  rd_kafka_resp_err_t err;

  if (object.Has("config")) {
    // Get the config Napi::Object that we can get parameters on
    Napi::Object config = object.Get("config").ToObject();

    // Get all of the keys of the object
    Napi::Array config_keys =
        config.GetPropertyNames(); // TODO getownprpertynames

    if (!config_keys.IsEmpty()) {
      for (size_t i = 0; i < config_keys.Length(); i++) {
        Napi::String config_key = config_keys.Get(i).ToString();
        Napi::Value config_value = config.Get(config_key);

        // If the config value is a string...
        if (config_value.IsString()) {
          std::string pKeyString = config_key.ToString().Utf8Value();

          std::string pValString = config_value.ToString().Utf8Value();

          err = rd_kafka_NewTopic_set_config(new_topic, pKeyString.c_str(),
                                             pValString.c_str());

          if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            errstr = rd_kafka_err2str(err);
            rd_kafka_NewTopic_destroy(new_topic);
            return NULL;
          }
        } else {
          errstr = "Config values must all be provided as strings.";
          rd_kafka_NewTopic_destroy(new_topic);
          return NULL;
        }
      }
    }
  }

  // Free it again cuz we malloc'd it.
  // free(errbuf);
  return new_topic;
}

rd_kafka_NewTopic_t **FromV8TopicObjectArray(Napi::Array) { return NULL; }

} // namespace Admin

} // namespace Conversion

namespace Util {
std::string FromV8String(Napi::String val) { return val.Utf8Value(); }
} // Namespace Util

} // namespace NodeKafka
