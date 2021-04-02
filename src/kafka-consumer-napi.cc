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

#include "src/kafka-consumer-napi.h"
#include "src/workers.h"

namespace NodeKafka {

/**
 * @brief KafkaConsumerNapi v8 wrapped object.
 *
 * Specializes the connection to wrap a consumer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

KafkaConsumerNapi::KafkaConsumerNapi(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<KafkaConsumerNapi>(info) {
  Napi::Env env = info.Env();
  if (!info.IsConstructCall()) {
    Napi::TypeError::New(env, "non-constructor invocation not supported")
        .ThrowAsJavaScriptException();
    return;
  }

  if (info.Length() < 2) {
    Napi::TypeError::New(env, "You must supply global and topic configuration")
        .ThrowAsJavaScriptException();
    return;
  }

  if (!info[0].IsObject()) {
    Napi::TypeError::New(env, "Global configuration data must be specified")
        .ThrowAsJavaScriptException();
    return;
  }

  if (!info[1].IsObject()) {
    Napi::TypeError::New(env, "Topic configuration must be specified")
        .ThrowAsJavaScriptException();
    return;
  }

  std::string errstr;

  Conf *gconfig =
      Conf::create(RdKafka::Conf::CONF_GLOBAL, (info[0].ToObject()), errstr);

  if (!gconfig) {
    Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
    return;
  }

  Conf *tconfig =
      Conf::create(RdKafka::Conf::CONF_TOPIC, (info[1].ToObject()), errstr);

  if (!tconfig) {
    delete gconfig;
    Napi::TypeError::New(env, errstr.c_str()).ThrowAsJavaScriptException();
    return;
  }
  consumer = new KafkaConsumer(gconfig, tconfig);
}

void KafkaConsumerNapi::Finalize(Napi::Env env) {
  // We only want to run this if it hasn't been run already
  delete consumer;
}

void KafkaConsumerNapi::Init(Napi::Env env, Napi::Object exports) {

  // v8::Local<v8::FunctionTemplate> tpl = Napi::Number::New(env, New);
  // tpl->SetClassName(Napi::String::New(env, "KafkaConsumerNapi"));
  // tpl->InstanceTemplate()->SetInternalFieldCount(1);
  Napi::Function func = DefineClass(
      env, "KafkaConsumer",
      {
          /*
           * Lifecycle events inherited from NodeKafka::Connection
           *
           * @sa NodeKafka::Connection
           */

          InstanceMethod("configureCallbacks",
                         &KafkaConsumerNapi::NodeConfigureCallbacks),

          /*
           * @brief Methods to do with establishing state
           */

          InstanceMethod("connect", &KafkaConsumerNapi::NodeConnect),
          InstanceMethod("disconnect", &KafkaConsumerNapi::NodeDisconnect),
          InstanceMethod("getMetadata", &KafkaConsumerNapi::NodeGetMetadata),
          InstanceMethod(
              "queryWatermarkOffsets",
              &KafkaConsumerNapi::NodeQueryWatermarkOffsets), // NOLINT
          InstanceMethod("offsetsForTimes",
                         &KafkaConsumerNapi::NodeOffsetsForTimes),
          InstanceMethod("getWatermarkOffsets",
                         &KafkaConsumerNapi::NodeGetWatermarkOffsets),

          /*
           * @brief Methods exposed to do with message retrieval
           */
          InstanceMethod("subscription", &KafkaConsumerNapi::NodeSubscription),
          InstanceMethod("subscribe", &KafkaConsumerNapi::NodeSubscribe),
          InstanceMethod("unsubscribe", &KafkaConsumerNapi::NodeUnsubscribe),
          InstanceMethod("consumeLoop", &KafkaConsumerNapi::NodeConsumeLoop),
          InstanceMethod("consume", &KafkaConsumerNapi::NodeConsume),
          InstanceMethod("seek", &KafkaConsumerNapi::NodeSeek),

          /**
           * @brief Pausing and resuming
           */
          InstanceMethod("pause", &KafkaConsumerNapi::NodePause),
          InstanceMethod("resume", &KafkaConsumerNapi::NodeResume),

          /*
           * @brief Methods to do with partition assignment / rebalancing
           */

          InstanceMethod("committed", &KafkaConsumerNapi::NodeCommitted),
          InstanceMethod("position", &KafkaConsumerNapi::NodePosition),
          InstanceMethod("assign", &KafkaConsumerNapi::NodeAssign),
          InstanceMethod("unassign", &KafkaConsumerNapi::NodeUnassign),
          InstanceMethod("assignments", &KafkaConsumerNapi::NodeAssignments),

          InstanceMethod("commit", &KafkaConsumerNapi::NodeCommit),
          InstanceMethod("commitSync", &KafkaConsumerNapi::NodeCommitSync),
          InstanceMethod("offsetsStore", &KafkaConsumerNapi::NodeOffsetsStore),
      });

  Napi::FunctionReference *constructor = new Napi::FunctionReference();
  *constructor = Napi::Persistent(func);
  env.SetInstanceData(constructor);
  exports.Set("KafkaConsumer", func);
}

Napi::Value
KafkaConsumerNapi::NodeOffsetsForTimes(const Napi::CallbackInfo &info) {
  return consumer->NodeOffsetsForTimes(info);
}
Napi::Value
KafkaConsumerNapi::NodeConfigureCallbacks(const Napi::CallbackInfo &info) {
  return consumer->NodeConfigureCallbacks(info);
}
Napi::Value KafkaConsumerNapi::NodeGetMetadata(const Napi::CallbackInfo &info) {
  return consumer->NodeGetMetadata(info);
}
Napi::Value
KafkaConsumerNapi::NodeQueryWatermarkOffsets(const Napi::CallbackInfo &info) {
  return consumer->NodeQueryWatermarkOffsets(info);
}

Napi::Object KafkaConsumerNapi::NewInstance(Napi::Env env, Napi::Value arg) {
  Napi::EscapableHandleScope scope(env);

  Napi::Object obj = env.GetInstanceData<Napi::FunctionReference>()->New({arg});
  return scope.Escape(napi_value(obj)).ToObject();
}

/* Node exposed methods */

Napi::Value KafkaConsumerNapi::NodeCommitted(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 3 || !info[0].IsArray()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify an array of topic partitions")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::vector<RdKafka::TopicPartition *> toppars =
      Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());
  Napi::Number maybeTimeout = info[1].As<Napi::Number>();
  int timeout_ms;

  if (maybeTimeout.IsEmpty()) {
    timeout_ms = 1000;
  } else {
    timeout_ms = maybeTimeout.Int32Value();
  }

  Napi::Function cb = info[2].As<Napi::Function>();

  new Workers::KafkaConsumerCommitted(cb, consumer, toppars, timeout_ms);
  return env.Null();
}

Napi::Value
KafkaConsumerNapi::NodeSubscription(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  Baton b = consumer->Subscription();

  if (b.err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
    // Let the JS library throw if we need to so the error can be more rich
    int error_code = static_cast<int>(b.err());
    return Napi::Number::New(env, error_code);
  }

  std::vector<std::string> *topics = b.data<std::vector<std::string> *>();

  Conversion::Topic::ToV8Array(env, *topics);

  delete topics;
  return env.Null();
}

Napi::Value KafkaConsumerNapi::NodePosition(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsArray()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify an array of topic partitions")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::vector<RdKafka::TopicPartition *> toppars =
      Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

  Baton b = consumer->Position(toppars);

  if (b.err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
    // Let the JS library throw if we need to so the error can be more rich
    int error_code = static_cast<int>(b.err());
    return Napi::Number::New(env, error_code);
  }

  Napi::Array array = Conversion::TopicPartition::ToV8Array(env, toppars);

  // Delete the underlying topic partitions
  RdKafka::TopicPartition::destroy(toppars);
  return array;
}

Napi::Value KafkaConsumerNapi::NodeAssignments(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  Baton b = consumer->RefreshAssignments();

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    // Let the JS library throw if we need to so the error can be more rich
    int error_code = static_cast<int>(b.err());
    return Napi::Number::New(env, error_code);
  }

  return Conversion::TopicPartition::ToV8Array(env, consumer->m_partitions);
}

Napi::Value KafkaConsumerNapi::NodeAssign(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsArray()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify an array of partitions")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Array partitions = info[0].As<Napi::Array>();
  std::vector<RdKafka::TopicPartition *> topic_partitions;

  for (unsigned int i = 0; i < partitions.Length(); ++i) {
    Napi::Value partition_obj_value = partitions.Get(i);
    if (!(partition_obj_value.IsObject())) {
      Napi::TypeError::New(env, "Must pass topic-partition objects")
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    Napi::Object partition_obj = partition_obj_value.As<Napi::Object>();

    // Got the object
    int64_t partition = GetParameter<int64_t>(partition_obj, "partition", -1);
    std::string topic = GetParameter<std::string>(partition_obj, "topic", "");

    if (!topic.empty()) {
      RdKafka::TopicPartition *part;

      if (partition < 0) {
        part = Connection::GetPartition(topic);
      } else {
        part = Connection::GetPartition(topic, partition);
      }

      // Set the default value to offset invalid. If provided, we will not set
      // the offset.
      int64_t offset = GetParameter<int64_t>(partition_obj, "offset",
                                             RdKafka::Topic::OFFSET_INVALID);
      if (offset != RdKafka::Topic::OFFSET_INVALID) {
        part->set_offset(offset);
      }

      topic_partitions.push_back(part);
    }
  }

  // Hand over the partitions to the consumer.
  Baton b = consumer->Assign(topic_partitions);

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    Napi::TypeError::New(env, RdKafka::err2str(b.err()).c_str())
        .ThrowAsJavaScriptException();
  }

  return Napi::Boolean::New(env, true);
}

Napi::Value KafkaConsumerNapi::NodeUnassign(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (!consumer->IsClosing() && !consumer->IsConnected()) {
    Napi::TypeError::New(env, "KafkaConsumerNapi is disconnected")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Baton b = consumer->Unassign();

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    Napi::TypeError::New(env, RdKafka::err2str(b.err()).c_str())
        .ThrowAsJavaScriptException();
  }

  return Napi::Boolean::New(env, true);
}

Napi::Value KafkaConsumerNapi::NodeUnsubscribe(const Napi::CallbackInfo &info) {

  Napi::Env env = info.Env();

  Baton b = consumer->Unsubscribe();

  return Napi::Number::New(env, static_cast<int>(b.err()));
}

Napi::Value KafkaConsumerNapi::NodeCommit(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  int error_code;

  if (!consumer->IsConnected()) {
    Napi::TypeError::New(env, "KafkaConsumerNapi is disconnected")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (info[0].IsNull() || info[0].IsUndefined()) {
    Baton b = consumer->Commit();
    error_code = static_cast<int>(b.err());
  } else if (info[0].IsArray()) {
    std::vector<RdKafka::TopicPartition *> toppars =
        Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

    Baton b = consumer->Commit(toppars);
    error_code = static_cast<int>(b.err());

    RdKafka::TopicPartition::destroy(toppars);
  } else if (info[0].IsObject()) {
    RdKafka::TopicPartition *toppar =
        Conversion::TopicPartition::FromV8Object(info[0].As<Napi::Object>());

    if (toppar == NULL) {
      Napi::TypeError::New(env, "Invalid topic partition provided")
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    Baton b = consumer->Commit(toppar);
    error_code = static_cast<int>(b.err());

    delete toppar;
  } else {
    Napi::TypeError::New(env, "First parameter must be an object or an array")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  return Napi::Number::New(env, error_code);
}

Napi::Value KafkaConsumerNapi::NodeCommitSync(const Napi::CallbackInfo &info) {

  int error_code;
  Napi::Env env = info.Env();

  if (!consumer->IsConnected()) {
    Napi::TypeError::New(env, "KafkaConsumerNapi is disconnected")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (info[0].IsNull() || info[0].IsUndefined()) {
    Baton b = consumer->CommitSync();
    error_code = static_cast<int>(b.err());
  } else if (info[0].IsArray()) {
    std::vector<RdKafka::TopicPartition *> toppars =
        Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

    Baton b = consumer->CommitSync(toppars);
    error_code = static_cast<int>(b.err());

    RdKafka::TopicPartition::destroy(toppars);
  } else if (info[0].IsObject()) {
    RdKafka::TopicPartition *toppar =
        Conversion::TopicPartition::FromV8Object(info[0].As<Napi::Object>());

    if (toppar == NULL) {
      Napi::TypeError::New(env, "Invalid topic partition provided")
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    Baton b = consumer->CommitSync(toppar);
    error_code = static_cast<int>(b.err());

    delete toppar;
  } else {
    Napi::TypeError::New(env, "First parameter must be an object or an array")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  return Napi::Number::New(env, error_code);
}

Napi::Value KafkaConsumerNapi::NodeSubscribe(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsArray()) {
    // Just throw an exception
    Napi::TypeError::New(env, "First parameter must be an array")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Array topicsArray = info[0].As<Napi::Array>();
  std::vector<std::string> topics =
      Conversion::Topic::ToStringVector(topicsArray); // NOLINT

  Baton b = consumer->Subscribe(topics);

  int error_code = static_cast<int>(b.err());
  return Napi::Number::New(env, error_code);
}

Napi::Value KafkaConsumerNapi::NodeSeek(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // If number of parameters is less than 3 (need topic partition, timeout,
  // and callback), we can't call this thing
  if (info.Length() < 3) {
    Napi::TypeError::New(
        env, "Must provide a topic partition, timeout, and callback")
        .ThrowAsJavaScriptException(); // NOLINT
    return env.Null();
  }

  if (!info[0].IsObject()) {
    Napi::TypeError::New(env, "Topic partition must be an object")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[1].IsNumber() && !info[1].IsNull()) {
    Napi::TypeError::New(env, "Timeout must be a number.")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[2].IsFunction()) {
    Napi::TypeError::New(env, "Callback must be a function")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  int timeout_ms;
  Napi::Number maybeTimeout = info[1].As<Napi::Number>();

  if (maybeTimeout.IsEmpty()) {
    timeout_ms = 1000;
  } else {
    timeout_ms = maybeTimeout.Int32Value();
    // Do not allow timeouts of less than 10. Providing 0 causes segfaults
    // because it makes it asynchronous.
    if (timeout_ms < 10) {
      timeout_ms = 10;
    }
  }

  const RdKafka::TopicPartition *toppar =
      Conversion::TopicPartition::FromV8Object(info[0].As<Napi::Object>());

  if (!toppar) {
    Napi::TypeError::New(env, "Invalid topic partition provided")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  Napi::Function cb = info[2].As<Napi::Function>();

  new Workers::KafkaConsumerSeek(cb, consumer, toppar, timeout_ms);
  return env.Null();
}

Napi::Value
KafkaConsumerNapi::NodeOffsetsStore(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // If number of parameters is less than 3 (need topic partition, timeout,
  // and callback), we can't call this thing
  if (info.Length() < 1) {
    Napi::TypeError::New(env, "Must provide a list of topic partitions")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[0].IsArray()) {
    Napi::TypeError::New(env, "Topic partition must be an array of objects")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::vector<RdKafka::TopicPartition *> toppars =
      Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

  Baton b = consumer->OffsetsStore(toppars);
  RdKafka::TopicPartition::destroy(toppars);

  int error_code = static_cast<int>(b.err());
  return Napi::Number::New(env, error_code);
}

Napi::Value KafkaConsumerNapi::NodePause(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // If number of parameters is less than 3 (need topic partition, timeout,
  // and callback), we can't call this thing
  if (info.Length() < 1) {
    Napi::TypeError::New(env, "Must provide a list of topic partitions")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[0].IsArray()) {
    Napi::TypeError::New(env, "Topic partition must be an array of objects")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::vector<RdKafka::TopicPartition *> toppars =
      Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

  Baton b = consumer->Pause(toppars);
  RdKafka::TopicPartition::destroy(toppars);

#if 0
    // Now iterate through and delete these toppars
    for (std::vector<RdKafka::TopicPartition*>::const_iterator it = toppars.begin();  // NOLINT
      it != toppars.end(); it++) {
      RdKafka::TopicPartition* toppar = *it;
      if (toppar->err() != RdKafka::ERR_NO_ERROR) {
        // Need to somehow transmit this information.
        // @TODO(webmakersteve)
      }
      delete toppar;
    }
#endif

  int error_code = static_cast<int>(b.err());
  return Napi::Number::New(env, error_code);
}

Napi::Value KafkaConsumerNapi::NodeResume(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // If number of parameters is less than 3 (need topic partition, timeout,
  // and callback), we can't call this thing
  if (info.Length() < 1) {
    Napi::TypeError::New(env, "Must provide a list of topic partitions")
        .ThrowAsJavaScriptException();
    return env.Null(); // NOLINT
  }

  if (!info[0].IsArray()) {
    Napi::TypeError::New(env, "Topic partition must be an array of objects")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::vector<RdKafka::TopicPartition *> toppars =
      Conversion::TopicPartition::FromV8Array(info[0].As<Napi::Array>());

  Baton b = consumer->Resume(toppars);

  // Now iterate through and delete these toppars
  for (std::vector<RdKafka::TopicPartition *>::const_iterator it =
           toppars.begin(); // NOLINT
       it != toppars.end(); it++) {
    RdKafka::TopicPartition *toppar = *it;
    if (toppar->err() != RdKafka::ERR_NO_ERROR) {
      // Need to somehow transmit this information.
      // @TODO(webmakersteve)
    }
    delete toppar;
  }

  int error_code = static_cast<int>(b.err());
  return Napi::Number::New(env, error_code);
}

Napi::Value KafkaConsumerNapi::NodeConsumeLoop(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 3) {
    // Just throw an exception
    Napi::TypeError::New(env, "Invalid number of parameters")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[0].IsNumber()) {
    Napi::TypeError::New(env, "Need to specify a timeout")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[1].IsNumber()) {
    Napi::TypeError::New(env, "Need to specify a sleep delay")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!info[2].IsFunction()) {
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  int timeout_ms;
  Napi::Number maybeTimeout = info[0].As<Napi::Number>();

  if (maybeTimeout.IsEmpty()) {
    timeout_ms = 1000;
  } else {
    timeout_ms = maybeTimeout.Int32Value();
  }
  int retry_read_ms;
  Napi::Number maybeSleep = info[1].As<Napi::Number>();

  if (maybeSleep.IsEmpty()) {
    retry_read_ms = 500;
  } else {
    retry_read_ms = maybeSleep.Int32Value();
  }

  Napi::Function cb = info[2].As<Napi::Function>();

  new Workers::KafkaConsumerConsumeLoop(cb, consumer, timeout_ms,
                                        retry_read_ms);
  return env.Null();
}

Napi::Value KafkaConsumerNapi::NodeConsume(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 2) {
    // Just throw an exception
    Napi::TypeError::New(env, "Invalid number of parameters")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  int timeout_ms;
  Napi::Number maybeTimeout = info[0].As<Napi::Number>();

  if (maybeTimeout.IsEmpty()) {
    timeout_ms = 1000;
  } else {
    timeout_ms = maybeTimeout.Int32Value();
  }

  if (info[1].IsNumber()) {
    if (!info[2].IsFunction()) {
      Napi::TypeError::New(env, "Need to specify a callback")
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    Napi::Number numMessagesMaybe = info[1].As<Napi::Number>();

    uint32_t numMessages;
    if (numMessagesMaybe.IsEmpty()) {
      Napi::TypeError::New(env, "Parameter must be a number over 0")
          .ThrowAsJavaScriptException();
      return env.Null();
    } else {
      numMessages = numMessagesMaybe.Int32Value();
    }

    Napi::Function cb = info[2].As<Napi::Function>();
    new Workers::KafkaConsumerConsumeNum(cb, consumer, numMessages,
                                         timeout_ms); // NOLINT
  } else {
    if (!info[1].IsFunction()) {
      Napi::TypeError::New(env, "Need to specify a callback")
          .ThrowAsJavaScriptException();
      return env.Null();
    }

    Napi::Function cb = info[1].As<Napi::Function>();
    new Workers::KafkaConsumerConsume(cb, consumer, timeout_ms);
  }
  return env.Null();
}

Napi::Value KafkaConsumerNapi::NodeConnect(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Function callback = info[0].As<Napi::Function>();
  new Workers::KafkaConsumerConnect(callback, consumer);
  return env.Null();
}

Napi::Value KafkaConsumerNapi::NodeDisconnect(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsFunction()) {
    // Just throw an exception
    Napi::TypeError::New(env, "Need to specify a callback")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Function cb = info[0].As<Napi::Function>();

  new Workers::KafkaConsumerDisconnect(cb, consumer);
  return env.Null();
}

Napi::Value
KafkaConsumerNapi::NodeGetWatermarkOffsets(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();

  // KafkaConsumerNapi* obj =
  // ObjectWrap::Unwrap<KafkaConsumerNapi>(info.This());

  if (!info[0].IsString()) {
    Napi::TypeError::New(env, "1st parameter must be a topic string")
        .ThrowAsJavaScriptException();
    ;
    return env.Null();
  }

  if (!info[1].IsNumber()) {
    Napi::TypeError::New(env, "2nd parameter must be a partition number")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  // Get string pointer for the topic name
  // The first parameter is the topic
  std::string topic_name = info[0].As<Napi::String>().Utf8Value();

  // Second parameter is the partition
  int32_t partition = info[1].As<Napi::Number>().Int32Value();

  // Set these ints which will store the return data
  int64_t low_offset;
  int64_t high_offset;

  Baton b = consumer->GetWatermarkOffsets(topic_name, partition, &low_offset,
                                          &high_offset);

  if (b.err() != RdKafka::ERR_NO_ERROR) {
    // Let the JS library throw if we need to so the error can be more rich
    int error_code = static_cast<int>(b.err());
    return Napi::Number::New(env, error_code);
  } else {
    Napi::Object offsetsObj = Napi::Object::New(env);
    offsetsObj.Set("lowOffset", Napi::Number::New(env, low_offset));
    offsetsObj.Set("highOffset", Napi::Number::New(env, high_offset));

    return offsetsObj;
  }
}

} // namespace NodeKafka
