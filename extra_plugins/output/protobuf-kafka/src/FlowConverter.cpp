/**
 * \file FlowConverter.cpp
 * \brief To Protobuf converter
 * \author Jaroslav Pesek
 * \date 2026
 */

#include "FlowConverter.hpp"

#include <arpa/inet.h>

namespace protobuf_kafka {

FlowConverter::FlowConverter(ProtoSchema& schema,
                             const TranslationTable& table,
                             PartitionMode mode)
    : m_table(table)
    , m_partition_mode(mode)
    , m_message(nullptr)
    , m_reflection(nullptr)
{
    m_message = schema.createMessage();
    m_reflection = m_message->GetReflection();
    m_buffer.reserve(4096);
}

FlowConverter::~FlowConverter()
{
    delete m_message;
}

bool
FlowConverter::convert(const fds_drec* rec,
                       const char** out_data,
                       size_t* out_len,
                       PartitionKey* partition_key)
{
    m_message->Clear();
    PartitionKey pk_local{};
    bool need_partition_key = (m_partition_mode == PartitionMode::RSS && partition_key);

    const auto& ipfix_ids = m_table.ipfixIds();
    const auto& entries = m_table.entries();

    for (size_t i = 0; i < ipfix_ids.size(); ++i) {
        const auto& [pen, id] = ipfix_ids[i];
        const FieldEntry& entry = entries[i];

        struct fds_drec_field field;
        if (fds_drec_find(const_cast<fds_drec*>(rec), pen, id, &field) == FDS_EOC) {
            continue;
        }
        setField(entry, field.data, field.size);

        if (need_partition_key && pen == IANA_PEN) {
            extractPartitionField(id, field.data, field.size, &pk_local);
        }
    }
    if (need_partition_key) {
        pk_local.valid = (pk_local.src_ip != nullptr || pk_local.dst_ip != nullptr);
        *partition_key = pk_local;
    }

    m_buffer.clear();
    if (!m_message->SerializeToString(&m_buffer)) {
        return false;
    }

    *out_data = m_buffer.data();
    *out_len = m_buffer.size();
    return true;
}

void
FlowConverter::setField(const FieldEntry& entry, const uint8_t* data, size_t size)
{
    const google::protobuf::FieldDescriptor* fd = entry.fd;

    bool is_datetime = (entry.ipfix_type == FDS_ET_DATE_TIME_SECONDS ||
                        entry.ipfix_type == FDS_ET_DATE_TIME_MILLISECONDS ||
                        entry.ipfix_type == FDS_ET_DATE_TIME_MICROSECONDS ||
                        entry.ipfix_type == FDS_ET_DATE_TIME_NANOSECONDS);

    switch (fd->type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        if (size == 8) {
            double val;
            memcpy(&val, data, 8);
            m_reflection->SetDouble(m_message, fd, val);
        }
        break;

    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        if (size == 4) {
            float val;
            memcpy(&val, data, 4);
            m_reflection->SetFloat(m_message, fd, val);
        }
        break;

    case google::protobuf::FieldDescriptor::TYPE_INT64:
    case google::protobuf::FieldDescriptor::TYPE_SINT64:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                m_reflection->SetInt64(m_message, fd, static_cast<int64_t>(ts_ms));
            }
        } else {
            int64_t val = 0;
            if (fds_get_int_be(data, size, &val) == FDS_OK) {
                m_reflection->SetInt64(m_message, fd, val);
            }
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64:
    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                m_reflection->SetUInt64(m_message, fd, ts_ms);
            }
        } else {
            uint64_t val = 0;
            if (fds_get_uint_be(data, size, &val) == FDS_OK) {
                m_reflection->SetUInt64(m_message, fd, val);
            }
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32:
    case google::protobuf::FieldDescriptor::TYPE_SINT32:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                m_reflection->SetInt32(m_message, fd, static_cast<int32_t>(ts_ms / 1000));
            }
        } else {
            int64_t val = 0;
            if (fds_get_int_be(data, size, &val) == FDS_OK) {
                m_reflection->SetInt32(m_message, fd, static_cast<int32_t>(val));
            }
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32:
    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                m_reflection->SetUInt32(m_message, fd, static_cast<uint32_t>(ts_ms / 1000));
            }
        } else {
            uint64_t val = 0;
            if (fds_get_uint_be(data, size, &val) == FDS_OK) {
                m_reflection->SetUInt32(m_message, fd, static_cast<uint32_t>(val));
            }
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            m_reflection->SetBool(m_message, fd, val != 0);
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING:
        m_reflection->SetString(m_message, fd,
                                std::string(reinterpret_cast<const char*>(data), size));
        break;

    case google::protobuf::FieldDescriptor::TYPE_BYTES:
        m_reflection->SetString(m_message, fd,
                                std::string(reinterpret_cast<const char*>(data), size));
        break;

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            m_reflection->SetEnumValue(m_message, fd, static_cast<int>(val));
        }
        break;
    }

    default:
        break;
    }
}

void
FlowConverter::extractPartitionField(uint16_t id, const uint8_t* data, size_t size, PartitionKey* key)
{
    switch (id) {
    case ID_SRC_IPV4:
        if (!key->src_ip) {
            key->src_ip = data;
            key->src_ip_len = size;
        }
        break;
    case ID_DST_IPV4:
        if (!key->dst_ip) {
            key->dst_ip = data;
            key->dst_ip_len = size;
        }
        break;
    case ID_SRC_IPV6:
        if (!key->src_ip) {
            key->src_ip = data;
            key->src_ip_len = size;
        }
        break;
    case ID_DST_IPV6:
        if (!key->dst_ip) {
            key->dst_ip = data;
            key->dst_ip_len = size;
        }
        break;
    case ID_SRC_PORT:
        if (size == 2) {
            key->src_port = ntohs(*reinterpret_cast<const uint16_t*>(data));
        }
        break;
    case ID_DST_PORT:
        if (size == 2) {
            key->dst_port = ntohs(*reinterpret_cast<const uint16_t*>(data));
        }
        break;
    case ID_PROTOCOL:
        if (size >= 1) {
            key->protocol = data[0];
        }
        break;
    default:
        break;
    }
}

} // namespace protobuf_kafka
