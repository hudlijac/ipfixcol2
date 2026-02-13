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

    m_entries.reserve(mappings.size());

    for (const auto& mapping : mappings) {
        const google::protobuf::FieldDescriptor* fd =
            schema.findField(mapping.proto_name);

        if (!fd) {
            throw std::runtime_error(
                "Protobuf field '" + mapping.proto_name +
                "' not found in message type");
        }

        const fds_iemgr_elem* root_elem =
            fds_iemgr_elem_find_id(iemgr, mapping.root_pen, mapping.root_id);
        if (!root_elem) {
            throw std::runtime_error(
                "IPFIX element for mapping '" + mapping.ipfix_spec + "' not found in IE manager");
        }

        MappingKey key;
        key.root_pen = mapping.root_pen;
        key.root_id = mapping.root_id;

        fds_iemgr_element_type value_type = root_elem->data_type;
        if (mapping.is_list) {
            if (root_elem->data_type != FDS_ET_BASIC_LIST) {
                throw std::runtime_error(
                    "Mapping '" + mapping.ipfix_spec +
                    "' uses list selector but root element is not basicList");
            }

            const fds_iemgr_elem* list_elem =
                fds_iemgr_elem_find_id(iemgr, mapping.list_pen, mapping.list_id);
            if (!list_elem) {
                throw std::runtime_error(
                    "List element in mapping '" + mapping.ipfix_spec + "' not found in IE manager");
            }

            if (!fd->is_repeated()) {
                throw std::runtime_error(
                    "Mapping '" + mapping.ipfix_spec + "' targets non-repeated protobuf field '" +
                    mapping.proto_name + "'");
            }

            key.has_list_elem = true;
            key.list_pen = mapping.list_pen;
            key.list_id = mapping.list_id;
            value_type = list_elem->data_type;
        }

        FieldEntry entry;
        entry.fd = fd;
        entry.proto_name = mapping.proto_name;
        entry.ipfix_spec = mapping.ipfix_spec;
        entry.is_list = mapping.is_list;
        entry.ipfix_type = value_type;

        size_t index = m_entries.size();
        m_entries.push_back(entry);

        const bool inserted = m_lookup.emplace(key, index).second;
        if (!inserted) {
            throw std::runtime_error(
                "Duplicate mapping key for IPFIX specification '" + mapping.ipfix_spec + "'");
        }

        IPX_CTX_DEBUG(ctx, "Mapping: %s (root PEN=%u, root ID=%u, list PEN=%u, list ID=%u) -> %s (proto type=%d)",
                      mapping.ipfix_spec.c_str(),
                      mapping.root_pen, mapping.root_id,
                      mapping.is_list ? mapping.list_pen : 0U,
                      mapping.is_list ? mapping.list_id : 0U,
                      mapping.proto_name.c_str(),
                      static_cast<int>(fd->type()));
    }

    IPX_CTX_INFO(ctx, "Translation table built with %zu field mappings",
                 m_entries.size());
}

const FieldEntry*
TranslationTable::lookup(const MappingKey& key) const
{
    auto it = m_lookup.find(key);
    if (it == m_lookup.end()) {
        return nullptr;
    }
    return &m_entries[it->second];
}

} // namespace protobuf_kafka
