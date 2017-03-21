//
// Created by rony on 17. 3. 17.
//

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>

namespace scidb
{
    class PhysicalBCBetween: public PhysicalOperator
    {
    public:
        PhysicalBCBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
                PhysicalOperator(logicalName, physicalName, parameters, schema)
        {

        }

        Coordinates getWindowStart(const std::shared_ptr<Query>& query) const
        {
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
            std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
            PhysicalBoundaries window(getWindowStart(query), getWindowEnd(query));

            return inputBoundaries[0].intersectWith(window);
        }

        std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>> &inputArrays, std::shared_ptr<Query> query)
        {
            checkOrUpdateIntervals(_schema, inputArrays[0]);


        }
    };

    DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBCBetween, "bc_between", "physicalBCBetween")
}   // namespace scidb
