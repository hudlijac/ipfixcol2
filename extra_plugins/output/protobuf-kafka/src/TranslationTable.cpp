/**
 * \file TranslationTable.cpp
 * \brief Pre-computed IPFIX to Protobuf field mapping table
 * \author Generated
 * \date 2026
 */

#include "TranslationTable.hpp"

#include <ipfixcol2.h>
#include <stdexcept>

namespace protobuf_kafka {

void
TranslationTable::build(const std::vector<FieldMapping>& mappings,
                        ProtoSchema& schema,
                        const fds_iemgr_t* iemgr,
                        ipx_ctx_t* ctx)
{
    m_lookup.clear();
    m_entries.clear();
    m_ipfix_ids.clear();

    m_entries.reserve(mappings.size());
    m_ipfix_ids.reserve(mappings.size());

    for (const auto& mapping : mappings) {
        const google::protobuf::FieldDescriptor* fd =
            schema.findField(mapping.proto_name);

        if (!fd) {
            throw std::runtime_error(
                "Protobuf field '" + mapping.proto_name +
                "' not found in message type");
        }

        fds_iemgr_element_type ipfix_type = FDS_ET_OCTET_ARRAY;  // Default
        const fds_iemgr_elem* elem =
            fds_iemgr_elem_find_id(iemgr, mapping.ipfix_pen, mapping.ipfix_id);
        if (elem) {
            ipfix_type = elem->data_type;
        }

        FieldEntry entry;
        entry.fd = fd;
        entry.proto_name = mapping.proto_name;
        entry.ipfix_type = ipfix_type;

        size_t index = m_entries.size();
        m_entries.push_back(entry);
        m_ipfix_ids.emplace_back(mapping.ipfix_pen, mapping.ipfix_id);

        uint64_t key = makeKey(mapping.ipfix_pen, mapping.ipfix_id);
        m_lookup[key] = index;

        IPX_CTX_DEBUG(ctx, "Mapping: %s (PEN=%u, ID=%u) -> %s (proto type=%d)",
                      mapping.ipfix_spec.c_str(),
                      mapping.ipfix_pen, mapping.ipfix_id,
                      mapping.proto_name.c_str(),
                      static_cast<int>(fd->type()));
    }

    IPX_CTX_INFO(ctx, "Translation table built with %zu field mappings",
                 m_entries.size());
}

const FieldEntry*
TranslationTable::lookup(uint32_t pen, uint16_t id) const
{
    uint64_t key = makeKey(pen, id);
    auto it = m_lookup.find(key);
    if (it == m_lookup.end()) {
        return nullptr;
    }
    return &m_entries[it->second];
}

} // namespace protobuf_kafka
