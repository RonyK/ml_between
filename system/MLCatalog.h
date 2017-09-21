/*
 * MLCatalog.h
 *
 * Created on :Mar 16, 2017
 * Author : RonyK
 */

#ifndef ML_BETWEEN_MLCATALOG_H
#define ML_BETWEEN_MLCATALOG_H

#include <system/SystemCatalog.h>

namespace scidb
{
    class MLCatalog : public SystemCatalog
    {
        void addMLArray(const ArrayDesc &arrayDesc);

        void _addMLArray(const ArrayDesc &arrayDesc);

        void _addMLArray(const ArrayDesc &arrayDesc, pqxx::basic_transaction* tr);

        void getMLArrays(std::vector<std::string> &array);
    };
}

#endif //ML_BETWEEN_MLCATALOG_H
