//
// Created by rony on 17. 3. 17.
//

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "BCBetweenArray.h"
#include <log4cxx/logger.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.bcbetween"));

    class PhysicalBCBetween: public PhysicalOperator
    {
    public:
        PhysicalBCBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
                PhysicalOperator(logicalName, physicalName, parameters, schema)
        {
            printf("PhysicalBCBetween()");
            LOG4CXX_DEBUG(logger, "PhysicalBCBetween()");
        }

        Coordinates getWindowStart(const std::shared_ptr<Query>& query) const
        {
            printf("PhysicalBCBetween.getWindowStart()");
            LOG4CXX_DEBUG(logger, "PhysicalBCBetween::getWindowStart()");

            Dimensions const& dims = _schema.getDimensions();
            size_t nDims = dims.size();
            Coordinates result(nDims);
            for(size_t i = 0; i < nDims; i++)
            {
                Value const& coord = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
                if(coord.isNull() || coord.get<int64_t>() < dims[i].getStartMin())
                {
                    result[i] = dims[i].getStartMin();
                } else
                {
                    result[i] = coord.get<int64_t>();
                }

            }

            return result;
        }

        Coordinates getWindowEnd(const std::shared_ptr<Query> &query) const
        {
            printf("PhysicalBCBetween.getWindowEnd()");
            LOG4CXX_DEBUG(logger, "PhysicalBCBetween::getWindowEnd()");

            Dimensions const& dims = _schema.getDimensions();
            size_t nDims = dims.size();
            Coordinates result(nDims);
            for(size_t i = 0; i < nDims; i++)
            {
                Value const& coord = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
                if(coord.isNull() || coord.get<int64_t>() > dims[i].getEndMax())
                {
                    result[i] = dims[i].getEndMax();
                } else
                {
                    result[i] = coord.get<int64_t>();
                }
            }

            return result;
        }

        virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> &inputBoundaries, const std::vector<ArrayDesc> &inputSchemas) const
        {
            printf("PhysicalBCBetween.getOutputBoundaries()");
            LOG4CXX_DEBUG(logger, "PhysicalBCBetween::getOutputBoundaries()");

            std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
            PhysicalBoundaries window(getWindowStart(query), getWindowEnd(query));

            return inputBoundaries[0].intersectWith(window);
        }

        std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>> &inputArrays, std::shared_ptr<Query> query)
        {
            printf("PhysicalBCBetween.execute()");
            LOG4CXX_DEBUG(logger, "PhysicalBCBetween::execute()");

            assert(inputArrays.size() == 1);
            checkOrUpdateIntervals(_schema, inputArrays[0]);

            std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

            Coordinates lowPos = getWindowStart(query);
            Coordinates highPos = getWindowEnd(query);
            SpatialRangesPtr spatialRangesPtr = std::make_shared<SpatialRanges>(lowPos.size());
            if (isDominatedBy(lowPos, highPos)) {
                spatialRangesPtr->_ranges.push_back(SpatialRange(lowPos, highPos));
            }
            return std::shared_ptr<Array>(std::make_shared<BCBetweenArray>(_schema, spatialRangesPtr, inputArray));
        }
    };

    REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalBCBetween, "bc_between", "PhysicalBCBetween");
}   // namespace scidb
